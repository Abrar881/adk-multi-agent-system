import sys
import os
import json
import time
import random
import functools
import logging
import asyncio
import threading
from datetime import datetime, timezone
from mcp.server.fastmcp import FastMCP

from dotenv import load_dotenv
load_dotenv()

# Ensure project root on sys.path when spawned as MCP subprocess (same fix as monday_server.py)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from servers.circuit_breaker import CircuitBreaker


def _maps_cb_alert(name: str, failures: int) -> None:
    try:
        from servers.alerting import alert_admin, CRITICAL
    except ModuleNotFoundError:
        from alerting import alert_admin, CRITICAL  # type: ignore[no-redef]
    alert_admin(CRITICAL, f"circuit_breaker_{name}_open",
                f"Google Maps API circuit breaker opened after {failures} failures")


_maps_breaker = CircuitBreaker("google_maps", failure_threshold=3, recovery_timeout=120,
                                on_open_callback=_maps_cb_alert)

# --- STRUCTURED LOGGING ---
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# --- LOGGING CONSTANTS ---
SERVICE_NAME = "ops_server"
AGENT_NAME = "ops_controller"

def _log(level: str, tool: str, payload: dict) -> None:
    """Emit structured JSON logs with required aggregation fields."""
    out = dict(payload or {})
    out.setdefault("service", SERVICE_NAME)
    out.setdefault("agent", AGENT_NAME)
    out.setdefault("tool", tool)
    getattr(logger, level)(json.dumps(out))

# --- CONFIGURATION ---
# Google Maps API
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY', '')
GOOGLE_MAPS_BASE_URL = "https://maps.googleapis.com/maps/api"

# Default buffer time for feasibility checks (minutes)
DEFAULT_BUFFER_MINUTES = 15

# --- RATE LIMITING ---
_MAPS_HOURLY_LIMIT = 100
_maps_calls: list[float] = []
_maps_calls_lock = threading.Lock()

def _check_maps_rate_limit() -> tuple[bool, str]:
    now = time.time()
    with _maps_calls_lock:
        # Remove calls older than 1 hour
        while _maps_calls and now - _maps_calls[0] > 3600:
            _maps_calls.pop(0)
        if len(_maps_calls) >= _MAPS_HOURLY_LIMIT:
            return False, f"Maps API rate limit reached ({_MAPS_HOURLY_LIMIT}/hour). Try again later."
        _maps_calls.append(now)
        return True, ""


mcp = FastMCP("AcmeOps")


def _error_response(*, tool: str, error_code: str, message: str, latency_ms: int | None = None, **details) -> str:
    """Return a standardized machine-readable error payload."""
    payload = {
        "status": "error",
        "error_code": error_code,
        "message": message[:200],
        "tool": tool,
        "service": SERVICE_NAME,
    }
    if latency_ms is not None:
        payload["latency_ms"] = latency_ms
    payload.update(details)
    return json.dumps(payload)


_OPS_RESULT_CAP = 8000

def _cap_tool_result(result: str) -> str:
    if len(result) <= _OPS_RESULT_CAP:
        return result
    omitted = len(result) - _OPS_RESULT_CAP
    return result[:_OPS_RESULT_CAP] + f' [truncated â€” {omitted} chars omitted]"'


_log("info", "__init__", {
    "event": "server_startup",
    "status": "initialized"
})

# --- RETRY DECORATOR ---
def retry(exceptions=(Exception,), tries=4, delay=1, backoff=2, jitter=0.1):
    """Retry with exponential backoff + jitter."""
    def deco(func):
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    sleep_time = _delay + random.uniform(-jitter, jitter)
                    _log("warning", func.__name__, {
                        "event": "retry",
                        "wait_seconds": sleep_time,
                        "error": str(e),
                        "attempts_remaining": _tries - 1
                    })
                    _invalidate_http_session()
                    time.sleep(max(0, sleep_time))
                    _tries -= 1
                    _delay *= backoff
            return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    sleep_time = _delay + random.uniform(-jitter, jitter)
                    _log("warning", func.__name__, {
                        "event": "retry",
                        "wait_seconds": sleep_time,
                        "error": str(e),
                        "attempts_remaining": _tries - 1
                    })
                    await asyncio.sleep(max(0, sleep_time))
                    _tries -= 1
                    _delay *= backoff
            return await func(*args, **kwargs)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return deco

# --- HELPER FUNCTIONS ---

import requests as _requests
_http_session = _requests.Session()


def _get_http_session():
    global _http_session
    if _http_session is None:
        _http_session = _requests.Session()
    return _http_session


def _invalidate_http_session() -> None:
    global _http_session
    _http_session = None


@retry(exceptions=(Exception,), tries=3, delay=1, backoff=2)
def _call_maps_with_retry(url: str, params: dict) -> dict:
    """Inner HTTP call with retry. Circuit breaker handled by caller."""
    response = _get_http_session().get(url, params=params, timeout=30)
    if response.status_code != 200:
        raise Exception(f"Google Maps API error (HTTP {response.status_code}): {response.text}")
    data = response.json()
    status = data.get("status", "")
    if status not in ("OK", "ZERO_RESULTS"):
        raise Exception(f"Google Maps API error: {data.get('error_message', status)}")
    return data


def call_google_maps_api(endpoint: str, params: dict) -> dict:
    """Call Google Maps API with circuit breaker and retry."""
    if not _maps_breaker.allow_request():
        return {"error": "google_maps circuit open - API unavailable (circuit_breaker)", "status": "circuit_open"}
    allowed, msg = _check_maps_rate_limit()
    if not allowed:
        return {"error": msg, "status": "rate_limited"}
    params["key"] = GOOGLE_MAPS_API_KEY
    url = f"{GOOGLE_MAPS_BASE_URL}/{endpoint}"
    try:
        data = _call_maps_with_retry(url, params)
        _maps_breaker.record_success()
        return data
    except Exception:
        _maps_breaker.record_failure()
        raise


# --- TOOLS ---

def _calculate_travel_time_sync(origin: str, destination: str, mode: str = "transit") -> str:
    """
    Calculate travel time between two locations using Google Maps Distance Matrix API.
    
    Args:
        origin: Starting address or coordinates
        destination: Ending address or coordinates
        mode: Travel mode - 'transit' (default), 'driving', 'walking', 'bicycling'
    
    Returns:
        JSON with travel time in minutes and distance
    """
    start_time = time.time()
    
    try:
        if not GOOGLE_MAPS_API_KEY:
            raise ValueError("GOOGLE_MAPS_API_KEY environment variable not set")
        
        params = {
            "origins": origin,
            "destinations": destination,
            "mode": mode,
            "departure_time": "now"
        }
        
        data = call_google_maps_api("distancematrix/json", params)

        if data.get("status") == "circuit_open":
            return _error_response(tool="calculate_travel_time", error_code="api_unavailable", message="Google Maps API temporarily unavailable")

        # Parse response
        rows = data.get("rows", [])
        if not rows:
            return _error_response(tool="calculate_travel_time", error_code="api_error", message="No route found between origin and destination")

        elements = rows[0].get("elements", [])
        if not elements:
            return _error_response(tool="calculate_travel_time", error_code="api_error", message="No route elements returned")

        element = elements[0]
        element_status = element.get("status", "")

        if element_status == "ZERO_RESULTS":
            return _error_response(tool="calculate_travel_time", error_code="api_error", message=f"No {mode} route available between locations")

        if element_status != "OK":
            return _error_response(tool="calculate_travel_time", error_code="api_error", message=f"Route calculation failed: {element_status}")
        
        duration = element.get("duration", {})
        distance = element.get("distance", {})
        
        # Use duration_in_traffic if available (for driving mode)
        if "duration_in_traffic" in element:
            duration = element["duration_in_traffic"]
        
        duration_minutes = duration.get("value", 0) // 60
        distance_km = distance.get("value", 0) / 1000
        
        result = {
            "status": "success",
            "origin": origin,
            "destination": destination,
            "mode": mode,
            "duration_minutes": duration_minutes,
            "duration_text": duration.get("text", ""),
            "distance_km": round(distance_km, 2),
            "distance_text": distance.get("text", ""),
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        }
        
        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "calculate_travel_time", {
            "event": "calculate_travel_time",
            "status": "success",
            "origin": origin,
            "destination": destination,
            "mode": mode,
            "duration_minutes": duration_minutes,
            "latency_ms": latency_ms
        })
        
        return json.dumps(result)
        
    except ValueError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "calculate_travel_time", {
            "event": "calculate_travel_time",
            "status": "config_error",
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="calculate_travel_time", error_code="config_error", message=str(e), latency_ms=latency_ms)
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "calculate_travel_time", {
            "event": "calculate_travel_time",
            "status": "api_error",
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="calculate_travel_time", error_code="api_error", message=f"Maps API error: {str(e)}", latency_ms=latency_ms)

@mcp.tool()
async def calculate_travel_time(origin: str, destination: str, mode: str = "transit") -> str:
    """
    Calculate travel time between two locations using Google Maps Distance Matrix API.

    Args:
        origin: Starting address or coordinates
        destination: Ending address or coordinates
        mode: Travel mode - 'transit' (default), 'driving', 'walking', 'bicycling'

    Returns:
        JSON with travel time in minutes and distance
    """
    return await asyncio.to_thread(_calculate_travel_time_sync, origin, destination, mode)


def _check_job_feasibility_sync(
    job_address_1: str,
    job_time_1: str,
    job_address_2: str,
    job_time_2: str,
    buffer_minutes: int = DEFAULT_BUFFER_MINUTES,
    mode: str = "transit"
) -> str:
    """
    Check if two consecutive jobs are feasible based on travel time.
    Per AGENT GUIDE: TransitTime + 15min Buffer < GapTime
    
    Args:
        job_address_1: Address of first job
        job_time_1: End time of first job (ISO format or HH:MM)
        job_address_2: Address of second job
        job_time_2: Start time of second job (ISO format or HH:MM)
        buffer_minutes: Buffer time in minutes (default: 15)
        mode: Travel mode - 'transit' (default) or 'driving'
    
    Returns:
        JSON with feasibility result and reasoning
    """
    start_time = time.time()
    
    try:
        if not GOOGLE_MAPS_API_KEY:
            raise ValueError("GOOGLE_MAPS_API_KEY environment variable not set")
        
        # Parse times - handle both HH:MM and ISO formats
        from datetime import datetime as dt
        
        def parse_time(time_str: str) -> dt:
            time_str = time_str.strip()
            # Try ISO format first
            try:
                return dt.fromisoformat(time_str.replace('Z', '+00:00'))
            except ValueError:
                pass
            # Try HH:MM format (assume today's date)
            try:
                parsed = dt.strptime(time_str, "%H:%M")
                today = dt.now(timezone.utc).date()
                return dt.combine(today, parsed.time(), tzinfo=timezone.utc)
            except ValueError:
                raise ValueError(f"Invalid time format: {time_str}. Use HH:MM or ISO format.")
        
        time_1 = parse_time(job_time_1)
        time_2 = parse_time(job_time_2)
        
        # Calculate gap between jobs in minutes
        gap_timedelta = time_2 - time_1
        gap_minutes = int(gap_timedelta.total_seconds() / 60)
        
        if gap_minutes <= 0:
            return _error_response(tool="check_job_feasibility", error_code="validation_error", message="Second job time must be after first job time", job_time_1=job_time_1, job_time_2=job_time_2)
        
        # Get travel time (sync helper â€” no event loop in this thread context)
        travel_result_json = _calculate_travel_time_sync(job_address_1, job_address_2, mode)
        travel_result = json.loads(travel_result_json)
        
        if travel_result.get("status") != "success":
            return _error_response(tool="check_job_feasibility", error_code="api_error", message=f"Could not calculate travel time: {travel_result.get('message', 'Unknown error')}", job_address_1=job_address_1, job_address_2=job_address_2)
        
        travel_minutes = travel_result.get("duration_minutes", 0)
        required_minutes = travel_minutes + buffer_minutes
        
        # Feasibility check: TransitTime + Buffer < GapTime
        is_feasible = required_minutes < gap_minutes
        slack_minutes = gap_minutes - required_minutes
        
        result = {
            "status": "success",
            "feasible": is_feasible,
            "gap_minutes": gap_minutes,
            "travel_minutes": travel_minutes,
            "buffer_minutes": buffer_minutes,
            "required_minutes": required_minutes,
            "slack_minutes": slack_minutes,
            "mode": mode,
            "job_1": {
                "address": job_address_1,
                "end_time": job_time_1
            },
            "job_2": {
                "address": job_address_2,
                "start_time": job_time_2
            },
            "reasoning": f"Travel ({travel_minutes}m) + Buffer ({buffer_minutes}m) = {required_minutes}m. Gap is {gap_minutes}m. {'Feasible' if is_feasible else 'NOT feasible'} ({slack_minutes}m {'slack' if is_feasible else 'short'}).",
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        }
        
        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "check_job_feasibility", {
            "event": "check_job_feasibility",
            "status": "success",
            "feasible": is_feasible,
            "gap_minutes": gap_minutes,
            "required_minutes": required_minutes,
            "slack_minutes": slack_minutes,
            "latency_ms": latency_ms
        })
        
        return json.dumps(result)
        
    except ValueError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "check_job_feasibility", {
            "event": "check_job_feasibility",
            "status": "validation_error",
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="check_job_feasibility", error_code="validation_error", message=str(e), latency_ms=latency_ms)
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "check_job_feasibility", {
            "event": "check_job_feasibility",
            "status": "api_error",
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="check_job_feasibility", error_code="api_error", message=f"Feasibility check error: {str(e)}", latency_ms=latency_ms)

@mcp.tool()
async def check_job_feasibility(
    job_address_1: str,
    job_time_1: str,
    job_address_2: str,
    job_time_2: str,
    buffer_minutes: int = DEFAULT_BUFFER_MINUTES,
    mode: str = "transit",
) -> str:
    """
    Check if two consecutive jobs are feasible based on travel time.
    Per AGENT GUIDE: TransitTime + 15min Buffer < GapTime

    Args:
        job_address_1: Address of first job
        job_time_1: End time of first job (ISO format or HH:MM)
        job_address_2: Address of second job
        job_time_2: Start time of second job (ISO format or HH:MM)
        buffer_minutes: Buffer time in minutes (default: 15)
        mode: Travel mode - 'transit' (default) or 'driving'

    Returns:
        JSON with feasibility result and reasoning
    """
    return await asyncio.to_thread(
        _check_job_feasibility_sync,
        job_address_1, job_time_1, job_address_2, job_time_2, buffer_minutes, mode,
    )


def _optimize_route_sync(jobs_json: str, start_location: str = "", end_location: str = "") -> str:
    """
    Optimize route for multiple jobs.
    For <=5 jobs: Uses nearest neighbor heuristic.
    For >5 jobs: Per AGENT GUIDE, should use OR-Tools TSP solver (placeholder for now).
    
    Respects "Fixed Appointment Times" (hard constraints) vs "Anytime Access" (soft constraints).
    
    Args:
        jobs_json: JSON array of jobs, each with:
            - id: Job identifier
            - address: Job location
            - time: Scheduled time (optional, for fixed appointments)
            - fixed: Boolean, true if appointment time is fixed (default: false)
        start_location: Optional starting location (depot)
        end_location: Optional ending location (return to depot)
    
    Returns:
        JSON with optimized route order and total estimated travel time
    """
    start_time = time.time()
    
    try:
        if not GOOGLE_MAPS_API_KEY:
            raise ValueError("GOOGLE_MAPS_API_KEY environment variable not set")
        
        # Parse jobs
        try:
            jobs = json.loads(jobs_json)
            if not isinstance(jobs, list):
                raise ValueError("jobs_json must be a JSON array")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid jobs_json: {str(e)}")
        
        if len(jobs) == 0:
            return json.dumps({
                "status": "success",
                "message": "No jobs to optimize",
                "route": [],
                "total_travel_minutes": 0
            })
        
        if len(jobs) == 1:
            return json.dumps({
                "status": "success",
                "route": [jobs[0]],
                "total_travel_minutes": 0,
                "algorithm": "single_job"
            })
        
        # Separate fixed and flexible jobs
        fixed_jobs = [j for j in jobs if j.get("fixed", False)]
        flexible_jobs = [j for j in jobs if not j.get("fixed", False)]

        # Sort fixed jobs by time
        def parse_job_time(job):
            time_str = job.get("time", "00:00")
            try:
                return datetime.strptime(time_str, "%H:%M")
            except ValueError:
                try:
                    return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                except ValueError:
                    return datetime.min

        fixed_jobs.sort(key=parse_job_time)

        def _nearest_neighbor(flex_jobs, origin):
            """Nearest-neighbor heuristic for flexible jobs."""
            optimized = []
            remaining = flex_jobs.copy()
            current_loc = origin if origin else (remaining[0]["address"] if remaining else "")
            if not origin and remaining:
                optimized.append(remaining.pop(0))
                current_loc = optimized[0]["address"]
            while remaining:
                best_idx, best_mins = 0, float('inf')
                for idx, job in enumerate(remaining):
                    t = json.loads(_calculate_travel_time_sync(current_loc, job["address"], "driving"))
                    if t.get("status") == "success":
                        mins = t.get("duration_minutes", float('inf'))
                        if mins < best_mins:
                            best_mins, best_idx = mins, idx
                nxt = remaining.pop(best_idx)
                nxt["travel_from_previous"] = best_mins if best_mins != float('inf') else None
                optimized.append(nxt)
                current_loc = nxt["address"]
            return optimized

        def _ortools_tsp(flex_jobs, origin):
            """OR-Tools TSP/VRP solver for >5 flexible jobs with time-window constraints."""
            from ortools.constraint_solver import routing_enums_pb2, pywrapcp

            all_locs = []
            if origin:
                all_locs.append(origin)
            all_locs.extend(job["address"] for job in flex_jobs)

            n = len(all_locs)
            depot = 0 if origin else 0  # depot index

            # Build travel-time matrix (minutes, integer)
            dist_matrix = [[0] * n for _ in range(n)]
            for i in range(n):
                for j in range(n):
                    if i == j:
                        continue
                    t = json.loads(_calculate_travel_time_sync(all_locs[i], all_locs[j], "driving"))
                    dist_matrix[i][j] = int(t.get("duration_minutes", 120)) if t.get("status") == "success" else 120

            # Time windows: fixed jobs get tight windows; flexible = full day (0-1439 min)
            def time_to_minutes(time_str):
                if not time_str:
                    return 0
                try:
                    t = datetime.strptime(time_str, "%H:%M")
                    return t.hour * 60 + t.minute
                except ValueError:
                    try:
                        t = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                        return t.hour * 60 + t.minute
                    except ValueError:
                        return 0

            time_windows = [(0, 1439)]  # depot: full day
            for job in flex_jobs:
                if job.get("fixed") and job.get("time"):
                    mins = time_to_minutes(job["time"])
                    time_windows.append((max(0, mins - 15), mins + 15))
                else:
                    time_windows.append((0, 1439))

            manager = pywrapcp.RoutingIndexManager(n, 1, depot)
            routing = pywrapcp.RoutingModel(manager)

            def distance_callback(from_index, to_index):
                return dist_matrix[manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]

            transit_cb_idx = routing.RegisterTransitCallback(distance_callback)
            routing.SetArcCostEvaluatorOfAllVehicles(transit_cb_idx)

            routing.AddDimension(
                transit_cb_idx,
                30,    # slack (wait time)
                1440,  # max total time
                False,
                "Time"
            )
            time_dim = routing.GetDimensionOrDie("Time")
            for loc_idx in range(n):
                index = manager.NodeToIndex(loc_idx)
                time_dim.CumulVar(index).SetRange(time_windows[loc_idx][0], time_windows[loc_idx][1])

            search_params = pywrapcp.DefaultRoutingSearchParameters()
            search_params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION
            search_params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
            search_params.time_limit.seconds = 5

            solution = routing.SolveWithParameters(search_params)
            if not solution:
                return None  # caller falls back to nearest neighbor

            # Extract route
            index = routing.Start(0)
            ordered = []
            while not routing.IsEnd(index):
                node = manager.IndexToNode(index)
                if node != depot or not origin:
                    job_idx = node - (1 if origin else 0)
                    if 0 <= job_idx < len(flex_jobs):
                        ordered.append(flex_jobs[job_idx])
                index = solution.Value(routing.NextVar(index))
            return ordered if ordered else None

        # Choose algorithm based on job count
        if len(flexible_jobs) > 5:
            try:
                optimized_flexible = _ortools_tsp(flexible_jobs, start_location) or _nearest_neighbor(flexible_jobs, start_location)
                algorithm = "ortools_tsp"
            except Exception as _e:
                _log("warning", "optimize_route", {"event": "ortools_fallback", "reason": str(_e)})
                optimized_flexible = _nearest_neighbor(flexible_jobs, start_location)
                algorithm = "nearest_neighbor_fallback"
        elif len(flexible_jobs) > 1:
            optimized_flexible = _nearest_neighbor(flexible_jobs, start_location)
            algorithm = "nearest_neighbor"
        else:
            optimized_flexible = flexible_jobs
            algorithm = "single_job" if len(flexible_jobs) == 1 else "nearest_neighbor"
        
        # Interleave fixed and flexible jobs: flexible jobs fill gaps between fixed appointments (M8)
        if not fixed_jobs:
            all_jobs = optimized_flexible
        elif not optimized_flexible:
            all_jobs = fixed_jobs
        else:
            # Distribute flexible jobs evenly into gaps between fixed appointments.
            # fixed_jobs are sorted by time; optimized_flexible are in route-optimal order.
            # Strategy: insert flexible jobs one-per-gap before each fixed appointment,
            # then append any remaining flexible jobs at the end.
            all_jobs = []
            flex_iter = iter(optimized_flexible)
            next_flex = next(flex_iter, None)
            for fixed in fixed_jobs:
                if next_flex is not None:
                    all_jobs.append(next_flex)
                    next_flex = next(flex_iter, None)
                all_jobs.append(fixed)
            while next_flex is not None:
                all_jobs.append(next_flex)
                next_flex = next(flex_iter, None)
        
        # Calculate total travel time
        total_travel = 0
        route = []
        prev_location = start_location if start_location else None
        
        for job in all_jobs:
            if prev_location:
                travel_json = _calculate_travel_time_sync(prev_location, job["address"], "transit")
                travel_data = json.loads(travel_json)
                if travel_data.get("status") == "success":
                    job["travel_from_previous"] = travel_data.get("duration_minutes", 0)
                    total_travel += job["travel_from_previous"]
            route.append(job)
            prev_location = job["address"]

        # Add return to end location if specified
        if end_location and prev_location:
            travel_json = _calculate_travel_time_sync(prev_location, end_location, "transit")
            travel_data = json.loads(travel_json)
            if travel_data.get("status") == "success":
                total_travel += travel_data.get("duration_minutes", 0)
        
        result = {
            "status": "success",
            "route": route,
            "total_travel_minutes": total_travel,
            "job_count": len(route),
            "fixed_appointments": len(fixed_jobs),
            "flexible_jobs": len(flexible_jobs),
            "algorithm": algorithm,
            "start_location": start_location or "first_job",
            "end_location": end_location or "last_job",
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        }
        
        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "optimize_route", {
            "event": "optimize_route",
            "status": "success",
            "job_count": len(route),
            "total_travel_minutes": total_travel,
            "algorithm": algorithm,
            "latency_ms": latency_ms
        })
        
        return json.dumps(result)
        
    except ValueError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "optimize_route", {
            "event": "optimize_route",
            "status": "validation_error",
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="optimize_route", error_code="validation_error", message=str(e), latency_ms=latency_ms)
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "optimize_route", {
            "event": "optimize_route",
            "status": "error",
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="optimize_route", error_code="api_error", message=f"Route optimization error: {str(e)}", latency_ms=latency_ms)

@mcp.tool()
async def optimize_route(jobs_json: str, start_location: str = "", end_location: str = "") -> str:
    """
    Optimize route for multiple jobs.
    For <=5 jobs: Uses nearest neighbor heuristic.
    For >5 jobs: Uses OR-Tools TSP solver per AGENT GUIDE.

    Args:
        jobs_json: JSON array of job objects with 'address', 'time', 'access_type' fields
        start_location: Starting location (optional, e.g. depot address)
        end_location: Ending location (optional)

    Returns:
        JSON with optimized job order and travel times
    """
    return await asyncio.to_thread(_optimize_route_sync, jobs_json, start_location, end_location)


@mcp.tool()
def get_pending_enroute_checks(job_date: str = "") -> str:
    """
    Get pending en-route checks for a given date where the clerk was sent a check but has not responded.

    Args:
        job_date: Date to check in YYYY-MM-DD format (defaults to today in Europe/London timezone)

    Returns:
        JSON with list of non-responders who have jobs starting before 10:00
    """
    start_time = time.time()
    try:
        from google.cloud import firestore as _fs
        _db = _fs.Client(project=os.getenv("GOOGLE_CLOUD_PROJECT", "YOUR_GCP_PROJECT_ID"))

        if not job_date:
            try:
                from zoneinfo import ZoneInfo
            except ImportError:
                from backports.zoneinfo import ZoneInfo
            job_date = datetime.now(ZoneInfo("Europe/London")).strftime("%Y-%m-%d")

        docs = (
            _db.collection("pending_enroute_checks")
            .where("date", "==", job_date)
            .where("sent", "==", True)
            .where("responded", "==", False)
            .limit(50)
            .stream()
        )

        non_responders = []
        for doc in docs:
            data = doc.to_dict()
            job_time = data.get("job_time", "")
            # Only flag clerks with jobs before 10:00
            if job_time and job_time < "10:00":
                non_responders.append({
                    "clerk_name": data.get("clerk_name", "Unknown"),
                    "job_time": job_time,
                    "job_address": data.get("job_address", ""),
                    "phone": data.get("phone", ""),
                    "check_id": doc.id,
                })

        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "get_pending_enroute_checks", {
            "event": "get_pending_enroute_checks",
            "status": "success",
            "date": job_date,
            "non_responders": len(non_responders),
            "latency_ms": latency_ms,
        })
        return _cap_tool_result(json.dumps({
            "status": "success",
            "date": job_date,
            "non_responders": non_responders,
            "count": len(non_responders),
        }))

    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "get_pending_enroute_checks", {
            "event": "get_pending_enroute_checks",
            "status": "error",
            "error": str(e),
            "latency_ms": latency_ms,
        })
        return _error_response(
            tool="get_pending_enroute_checks",
            error_code="api_error",
            message=f"Firestore query error: {str(e)}",
            latency_ms=latency_ms,
        )


@mcp.tool()
def get_ops_status() -> str:
    """
    Get current Ops server status and configuration.
    
    Returns:
        JSON with server status and configuration info
    """
    start_time = time.time()
    
    try:
        maps_configured = bool(GOOGLE_MAPS_API_KEY)

        status = {
            "status": "success",
            "server_status": "healthy" if maps_configured else "partially_configured",
            "configuration": {
                "google_maps_api_key_set": maps_configured,
                "default_buffer_minutes": DEFAULT_BUFFER_MINUTES,
            },
            "capabilities": {
                "travel_time_calculation": maps_configured,
                "job_feasibility_check": maps_configured,
                "route_optimization": maps_configured,
                "contractor_status_checks": "via whatsapp_server",
                "admin_alerts": "via whatsapp_server"
            },
            "timestamp_utc": datetime.now(timezone.utc).isoformat()
        }
        
        latency_ms = int((time.time() - start_time) * 1000)
        _log("info", "get_ops_status", {
            "event": "get_ops_status",
            "status": "success",
            "server_status": status["server_status"],
            "latency_ms": latency_ms
        })
        
        return json.dumps(status)
        
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log("error", "get_ops_status", {
            "event": "get_ops_status",
            "status": "error",
            "error": str(e),
            "latency_ms": latency_ms
        })
        return _error_response(tool="get_ops_status", error_code="api_error", message=f"Status check error: {str(e)}", latency_ms=latency_ms)

def startup_health_check() -> bool:
    """
    Phase 3 requirement: Startup health check.
    Verify critical configuration is present.
    """
    warnings = []

    if not GOOGLE_MAPS_API_KEY:
        _log("warning", "startup_health_check", {
            "event": "config_warning",
            "warning": "GOOGLE_MAPS_API_KEY not set. Travel time and route optimization will not work.",
            "status": "degraded"
        })
        warnings.append("GOOGLE_MAPS_API_KEY")

    if warnings:
        _log("warning", "startup_health_check", {
            "event": "health_check_partial",
            "status": "degraded",
            "missing_config": warnings
        })
        return False
    _log("info", "startup_health_check", {
        "event": "health_check_passed",
        "status": "healthy"
    })
    return True

if __name__ == "__main__":
    _log("info", "__main__", {
        "event": "server_start",
        "message": "Ops MCP server starting"
    })
    
    startup_health_check()
    
    _log("info", "__main__", {
        "event": "server_ready",
        "status": "ready"
    })
    mcp.run()
