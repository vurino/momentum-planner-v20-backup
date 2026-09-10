from fastapi import FastAPI, APIRouter, HTTPException
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional
import uuid
from datetime import datetime, date, timedelta
from contextlib import asynccontextmanager
from scheduler import init_scheduler, stop_scheduler, generate_tasks_for_date
import analytics


ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Create the main app without a prefix
@asynccontextmanager
async def lifespan(app: FastAPI):
    await _backfill_daily_task_names(db)
    await _heal_premature_skips(db)
    await generate_tasks_for_date(db, date.today())
    init_scheduler(db)
    yield
    stop_scheduler()
    client.close()

app = FastAPI(lifespan=lifespan)

# Root-level health check (not under /api prefix)
@app.get("/health")
async def root_health_check():
    """Health check endpoint at root level for deployment verification"""
    return {"status": "healthy", "service": "momentum-planner-api"}

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")


# Define Models
class ScheduleSlot(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    label: str
    icon: str = "clock"  # Default icon
    start_time: str  # HH:MM format
    end_time: str  # HH:MM format
    group: str = "general"
    order_index: int
    days: List[str] = Field(default_factory=lambda: ["mon", "tue", "wed", "thu", "fri", "sat", "sun"])  # Days when this slot is active
    specific_date: Optional[str] = None  # One-off task: runs only on this YYYY-MM-DD; days is ignored
    notes: Optional[str] = None  # Optional notes field

class ScheduleSlotCreate(BaseModel):
    label: str
    icon: str = "clock"
    start_time: str
    end_time: str
    group: str = "general"
    order_index: int
    days: List[str] = Field(default_factory=lambda: ["mon", "tue", "wed", "thu", "fri", "sat", "sun"])
    specific_date: Optional[str] = None
    notes: Optional[str] = None

class ScheduleSlotUpdate(BaseModel):
    label: Optional[str] = None
    icon: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    group: Optional[str] = None
    order_index: Optional[int] = None
    days: Optional[List[str]] = None
    specific_date: Optional[str] = None
    notes: Optional[str] = None

class DailyTask(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    date: str  # YYYY-MM-DD format
    slot_id: str
    completed: bool = False
    skipped: bool = False       # Never attempted — counts against completion
    stopped: bool = False       # Attempted but not finished — counts against completion
    auto_skipped: bool = False  # Skip was applied automatically (window expired untouched)
    notes: Optional[str] = None  # Per-day notes for this task
    name: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration: Optional[int] = None
    started_at: Optional[str] = None    # ISO timestamp: user tapped Start
    completed_at: Optional[str] = None  # ISO timestamp: marked complete
    stopped_at: Optional[str] = None    # ISO timestamp: stopped (manual or auto)

class DailyTaskUpdate(BaseModel):
    completed: Optional[bool] = None
    skipped: Optional[bool] = None
    stopped: Optional[bool] = None
    auto_skipped: Optional[bool] = None
    notes: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    stopped_at: Optional[str] = None

class BulkSlotsUpdate(BaseModel):
    slots: List[ScheduleSlot]

class ImportData(BaseModel):
    schedule_slots: List[dict] = Field(default_factory=list)
    daily_tasks: List[dict] = Field(default_factory=list)

# Default schedule template - weekdays only for work activities
DEFAULT_SCHEDULE = [
    {"label": "Breakfast", "icon": "restaurant", "start_time": "07:00", "end_time": "08:00", "group": "morning", "order_index": 0, "days": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]},
    {"label": "Morning Focus", "icon": "sunny", "start_time": "08:00", "end_time": "09:00", "group": "morning", "order_index": 1, "days": ["mon", "tue", "wed", "thu", "fri"]},
    {"label": "Job Search", "icon": "briefcase", "start_time": "09:00", "end_time": "10:30", "group": "work", "order_index": 2, "days": ["mon", "tue", "wed", "thu", "fri"]},
    {"label": "Break", "icon": "cafe", "start_time": "10:30", "end_time": "10:45", "group": "break", "order_index": 3, "days": ["mon", "tue", "wed", "thu", "fri"]},
    {"label": "Trading Block 1", "icon": "trending-up", "start_time": "10:45", "end_time": "12:00", "group": "trading", "order_index": 4, "days": ["mon", "tue", "wed", "thu", "fri"]},
    {"label": "Trade Review / Journal", "icon": "book", "start_time": "12:00", "end_time": "13:00", "group": "trading", "order_index": 5, "days": ["mon", "tue", "wed", "thu", "fri"]},
    {"label": "Workout", "icon": "fitness", "start_time": "13:00", "end_time": "14:00", "group": "health", "order_index": 6, "days": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]},
    {"label": "Quick meal", "icon": "fast-food", "start_time": "14:00", "end_time": "14:30", "group": "break", "order_index": 7, "days": ["mon", "tue", "wed", "thu", "fri"]},
    {"label": "Trading Block 2", "icon": "analytics", "start_time": "14:30", "end_time": "16:00", "group": "trading", "order_index": 8, "days": ["mon", "tue", "wed", "thu", "fri"]},
    {"label": "Break", "icon": "cafe", "start_time": "16:00", "end_time": "16:15", "group": "break", "order_index": 9, "days": ["mon", "tue", "wed", "thu", "fri"]},
    {"label": "Trading Block 3", "icon": "settings", "start_time": "16:15", "end_time": "17:30", "group": "trading", "order_index": 10, "days": ["mon", "tue", "wed", "thu", "fri"]},
    {"label": "Create", "icon": "code", "start_time": "17:30", "end_time": "18:00", "group": "creative", "order_index": 11, "days": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]},
    {"label": "Dinner + Wind down", "icon": "moon", "start_time": "18:00", "end_time": "21:30", "group": "evening", "order_index": 12, "days": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]},
    {"label": "Sleep target", "icon": "bed", "start_time": "22:30", "end_time": "23:59", "group": "evening", "order_index": 13, "days": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]},
]

# Helper to get day of week abbreviation from date string
def get_day_abbr(date_str: str) -> str:
    """Convert date string to day abbreviation (mon, tue, etc)"""
    from datetime import datetime
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    return days[date_obj.weekday()]

def _slot_duration_minutes(start_time: str, end_time: str) -> int:
    """Minutes between two HH:MM times, wrapping past midnight"""
    sh, sm = map(int, start_time.split(":"))
    eh, em = map(int, end_time.split(":"))
    mins = (eh * 60 + em) - (sh * 60 + sm)
    if mins <= 0:
        mins += 24 * 60
    return mins

async def _enrich_tasks_with_slot_info(tasks: list) -> list:
    """Attach name/start_time/end_time/duration from each task's schedule slot"""
    slot_ids = {t["slot_id"] for t in tasks}
    slots = await db.schedule_slots.find({"id": {"$in": list(slot_ids)}}).to_list(100)
    slots_by_id = {s["id"]: s for s in slots}

    enriched = []
    for t in tasks:
        slot = slots_by_id.get(t["slot_id"])
        if slot:
            t["name"] = slot.get("label")
            t["start_time"] = slot.get("start_time")
            t["end_time"] = slot.get("end_time")
            t["duration"] = _slot_duration_minutes(slot["start_time"], slot["end_time"])
        enriched.append(t)
    return enriched


async def _backfill_daily_task_names(mongo_db):
    """One-time startup migration: older daily_tasks rows only stored a slot_id
    and relied on a live join to schedule_slots to show a name. If that slot was
    later edited or deleted, the row would render blank forever. This bakes
    name/start_time/end_time/duration onto every row that's missing them, and
    drops rows whose source activity no longer exists (unrecoverable)."""
    tasks = await mongo_db.daily_tasks.find({"name": None}).to_list(100000)
    if not tasks:
        return

    slot_ids = {t["slot_id"] for t in tasks}
    slots = await mongo_db.schedule_slots.find({"id": {"$in": list(slot_ids)}}).to_list(1000)
    slots_by_id = {s["id"]: s for s in slots}

    fixed, dropped = 0, 0
    for t in tasks:
        slot = slots_by_id.get(t["slot_id"])
        if slot:
            await mongo_db.daily_tasks.update_one(
                {"id": t["id"]},
                {"$set": {
                    "name": slot.get("label"),
                    "start_time": slot.get("start_time"),
                    "end_time": slot.get("end_time"),
                    "duration": _slot_duration_minutes(slot["start_time"], slot["end_time"]),
                }},
            )
            fixed += 1
        else:
            # Mark orphaned tasks instead of deleting - preserves data integrity
            if not t.get("orphaned"):
                await mongo_db.daily_tasks.update_one(
                    {"id": t["id"]},
                    {"$set": {"orphaned": True, "name": t.get("name", "Deleted Task")}},
                )
                dropped += 1

    logging.getLogger(__name__).info(
        f"[Startup] Backfilled {fixed} daily tasks, marked {dropped} orphaned tasks"
    )


async def _heal_premature_skips(mongo_db):
    """One-time repair for skips applied by the old UTC-date bug: the frontend
    used the UTC calendar date while comparing task windows against the local
    clock, so during a local evening it could fetch tomorrow's list and mass-skip
    it. Those bad skips predate the auto_skipped marker, so any skipped-but-not-
    completed task dated today-or-later without that marker gets unskipped.
    New skips always carry auto_skipped (true for automatic, false for manual),
    so they are never touched by this."""
    today_server = datetime.utcnow().date().isoformat()
    res = await mongo_db.daily_tasks.update_many(
        {
            "date": {"$gte": today_server},
            "skipped": True,
            "completed": False,
            "auto_skipped": {"$exists": False},
        },
        {"$set": {"skipped": False}},
    )
    if res.modified_count:
        logging.getLogger(__name__).info(
            f"[Startup] Healed {res.modified_count} prematurely skipped tasks"
        )


# Routes
@api_router.get("/")
async def root():
    return {"message": "Momentum Planner API"}

@api_router.get("/health")
async def health_check():
    """Health check endpoint for deployment verification"""
    return {"status": "healthy", "service": "momentum-planner-api"}

# Schedule Slots endpoints
@api_router.get("/schedule-slots", response_model=List[ScheduleSlot])
async def get_schedule_slots():
    """Get all schedule slots (template)"""
    slots = await db.schedule_slots.find().sort("order_index", 1).to_list(100)

    # Ensure all slots have the 'days' field (migration for existing data)
    result = []
    for slot in slots:
        if 'days' not in slot:
            slot['days'] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        if 'notes' not in slot:
            slot['notes'] = None
        if 'specific_date' not in slot:
            slot['specific_date'] = None
        result.append(ScheduleSlot(**slot))

    return result

@api_router.post("/schedule-slots", response_model=ScheduleSlot)
async def create_schedule_slot(slot_input: ScheduleSlotCreate):
    """Create a new schedule slot"""
    slot = ScheduleSlot(**slot_input.model_dump())
    await db.schedule_slots.insert_one(slot.model_dump())
    return slot

@api_router.put("/schedule-slots/{slot_id}", response_model=ScheduleSlot)
async def update_schedule_slot(slot_id: str, slot_update: ScheduleSlotUpdate):
    """Update a schedule slot"""
    update_data = {k: v for k, v in slot_update.model_dump().items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No update data provided")

    # Empty string is the explicit "clear" sentinel for specific_date
    # (None can't be used because None means "field not provided").
    if update_data.get("specific_date") == "":
        update_data["specific_date"] = None

    result = await db.schedule_slots.update_one(
        {"id": slot_id},
        {"$set": update_data}
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Slot not found")

    updated_slot = await db.schedule_slots.find_one({"id": slot_id})
    return ScheduleSlot(**updated_slot)

@api_router.delete("/schedule-slots/{slot_id}")
async def delete_schedule_slot(slot_id: str):
    """Delete a schedule slot"""
    result = await db.schedule_slots.delete_one({"id": slot_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Slot not found")
    return {"message": "Slot deleted successfully"}

@api_router.put("/schedule-slots/bulk/update")
async def bulk_update_slots(data: BulkSlotsUpdate):
    """Bulk update/replace all schedule slots"""
    # Clear existing slots
    await db.schedule_slots.delete_many({})

    # Insert new slots
    for slot in data.slots:
        await db.schedule_slots.insert_one(slot.model_dump())

    return {"message": "Slots updated successfully", "count": len(data.slots)}

@api_router.post("/schedule-slots/reset")
async def reset_schedule_slots():
    """Reset schedule slots to default"""
    await db.schedule_slots.delete_many({})

    for slot_data in DEFAULT_SCHEDULE:
        slot = ScheduleSlot(**slot_data)
        await db.schedule_slots.insert_one(slot.model_dump())

    slots = await db.schedule_slots.find().sort("order_index", 1).to_list(100)
    return [ScheduleSlot(**slot) for slot in slots]


# Daily Tasks endpoints
@api_router.get("/daily-tasks/{date_str}", response_model=List[DailyTask])
async def get_daily_tasks(date_str: str, client_today: Optional[str] = None):
    """Get daily tasks for a specific date. Creates tasks from template if they don't exist.

    client_today is the caller's local date (YYYY-MM-DD). When date_str is
    strictly before it, that day is definitively over — any task still
    sitting at completed=False/stopped=False/skipped=False was never
    attempted, so it's resolved to skipped (auto_skipped) before returning."""
    tasks = await db.daily_tasks.find({"date": date_str}).to_list(100)

    # If this exact date was explicitly cleared via "Clear today's tasks" in
    # Settings, don't bring back tasks for the activities that existed at
    # that moment — but a genuinely new activity created afterward (a fresh
    # slot_id) should still show up normally, so the check is per-slot, not
    # a blanket "skip everything for this date."
    cleared_marker = await db.app_meta.find_one({"key": "cleared_today_date"})
    was_cleared = bool(cleared_marker and cleared_marker.get("value") == date_str)
    cleared_slot_ids = set(cleared_marker.get("slot_ids", [])) if was_cleared else set()

    # This reconciliation always runs — not just when `tasks` is empty.
    # Originally it was gated behind `if not tasks or was_cleared`, on the
    # assumption tasks only ever need generating once per day. But a new
    # Routine activity can be created any time during the day, after today
    # already has other tasks — gating this away meant it silently never got
    # a task (and so never showed up in Today) until the gate re-opened
    # (tomorrow, or after "Clear today's tasks"). Checking every slot against
    # existing_slot_ids is cheap and inserts nothing for slots already
    # represented, so running it unconditionally is safe.
    day_abbr = get_day_abbr(date_str)
    slots = await db.schedule_slots.find().sort("order_index", 1).to_list(100)
    existing_slot_ids = {t["slot_id"] for t in tasks}
    inserted_any = False

    for slot in slots:
        if slot["id"] in existing_slot_ids:
            continue  # already has a task for this date
        if was_cleared and slot["id"] in cleared_slot_ids:
            continue  # deliberately cleared for today; don't bring it back
        # One-off slots only apply on their exact date; recurring slots
        # apply on their configured weekdays.
        specific = slot.get('specific_date')
        if specific:
            if specific != date_str:
                continue
        else:
            slot_days = slot.get('days', ["mon", "tue", "wed", "thu", "fri", "sat", "sun"])
            if day_abbr not in slot_days:
                continue
        task = DailyTask(
            date=date_str,
            slot_id=slot["id"],
            completed=False,
            name=slot.get("label"),
            start_time=slot.get("start_time"),
            end_time=slot.get("end_time"),
            duration=_slot_duration_minutes(slot["start_time"], slot["end_time"]),
            # Seeds today's note from the Routine activity's note the first
            # time this task is generated. After that it's this task's own
            # notes field (per-day, user-editable) — a later edit to the
            # Routine note won't overwrite whatever the user has already
            # typed for a day that's already been generated.
            notes=slot.get("notes"),
        )
        await db.daily_tasks.insert_one(task.model_dump())
        inserted_any = True

    if inserted_any:
        tasks = await db.daily_tasks.find({"date": date_str}).to_list(100)

    if client_today and date_str < client_today:
        await db.daily_tasks.update_many(
            {"date": date_str, "completed": False, "stopped": False, "skipped": False},
            {"$set": {"skipped": True, "auto_skipped": True}},
        )
        tasks = await db.daily_tasks.find({"date": date_str}).to_list(100)

    tasks = await _enrich_tasks_with_slot_info(tasks)
    return [DailyTask(**task) for task in tasks]

@api_router.patch("/daily-tasks/{task_id}", response_model=DailyTask)
@api_router.put("/daily-tasks/{task_id}", response_model=DailyTask)
async def update_daily_task(task_id: str, task_update: DailyTaskUpdate, client_today: Optional[str] = None):
    """Update a daily task (status, timestamps, or notes).

    client_today is the caller's local date (YYYY-MM-DD); status changes on
    dates after it are rejected — you can't complete/stop/skip a future task.
    Without it, a lenient UTC+1day bound is used (server can't know the
    caller's timezone)."""
    update_data = {k: v for k, v in task_update.model_dump().items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No update data provided")

    existing = await db.daily_tasks.find_one({"id": task_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Task not found")

    status_keys = {"completed", "skipped", "stopped"}
    if status_keys & set(update_data.keys()):
        limit = client_today or (datetime.utcnow().date() + timedelta(days=1)).isoformat()
        if existing["date"] > limit:
            raise HTTPException(status_code=400, detail="Cannot set task status for a future date")

    await db.daily_tasks.update_one(
        {"id": task_id},
        {"$set": update_data}
    )

    updated_task = await db.daily_tasks.find_one({"id": task_id})
    enriched = await _enrich_tasks_with_slot_info([updated_task])
    return DailyTask(**enriched[0])

@api_router.get("/daily-progress/{date_str}")
async def get_daily_progress(date_str: str):
    """Get progress summary for a specific date"""
    tasks = await db.daily_tasks.find({"date": date_str}).to_list(100)

    if not tasks:
        # Get tasks (this will create them if they don't exist)
        await get_daily_tasks(date_str)
        tasks = await db.daily_tasks.find({"date": date_str}).to_list(100)

    # Every listed task counts toward the total — skipped and stopped tasks
    # lower the day's percentage rather than shrinking the denominator.
    total = len(tasks)
    completed = sum(1 for t in tasks if t.get("completed", False))
    percentage = round((completed / total * 100) if total > 0 else 0)

    return {
        "date": date_str,
        "total": total,
        "completed": completed,
        "percentage": percentage
    }

@api_router.get("/monthly-progress/{year}/{month}")
async def get_monthly_progress(year: int, month: int):
    """Get progress summary for each day in a month.

    Days with no daily_tasks records yet (the scheduler never ran that
    night, or the date was never opened) are reported with tracked=False
    and a total computed from the current Routine template, so the
    calendar can tell "not tracked" apart from "tracked and incomplete"
    instead of just showing them as empty."""
    import calendar

    num_days = calendar.monthrange(year, month)[1]
    progress_data = []
    slots = await db.schedule_slots.find().to_list(100)

    for day in range(1, num_days + 1):
        date_str = f"{year:04d}-{month:02d}-{day:02d}"
        tasks = await db.daily_tasks.find({"date": date_str}).to_list(100)

        if tasks:
            total = len(tasks)
            completed = sum(1 for t in tasks if t.get("completed", False))
            progress_data.append({
                "date": date_str,
                "day": day,
                "total": total,
                "completed": completed,
                "percentage": round((completed / total * 100) if total > 0 else 0),
                "tracked": True,
            })
        else:
            day_abbr = get_day_abbr(date_str)
            expected = 0
            for slot in slots:
                specific = slot.get("specific_date")
                if specific:
                    if specific == date_str:
                        expected += 1
                else:
                    slot_days = slot.get("days", ["mon", "tue", "wed", "thu", "fri", "sat", "sun"])
                    if day_abbr in slot_days:
                        expected += 1
            progress_data.append({
                "date": date_str,
                "day": day,
                "total": expected,
                "completed": 0,
                "percentage": 0,
                "tracked": False,
            })

    return progress_data

@api_router.get("/weekly-summary/{date_str}")
async def get_weekly_summary(date_str: str, week_starts_monday: bool = True):
    """Get weekly summary (7 days ending on the given date or a Mon-Sun/Sun-Sat week)"""
    from datetime import datetime, timedelta

    end_date = datetime.strptime(date_str, "%Y-%m-%d")

    # Calculate the start of the week based on preference
    if week_starts_monday:
        # Find the Monday of this week
        days_since_monday = end_date.weekday()
        week_start = end_date - timedelta(days=days_since_monday)
        week_end = week_start + timedelta(days=6)
    else:
        # Find the Sunday of this week (week starts on Sunday)
        days_since_sunday = (end_date.weekday() + 1) % 7
        week_start = end_date - timedelta(days=days_since_sunday)
        week_end = week_start + timedelta(days=6)

    weekly_data = []

    for i in range(7):
        day_date = week_start + timedelta(days=i)
        day_str = day_date.strftime("%Y-%m-%d")
        tasks = await db.daily_tasks.find({"date": day_str}).to_list(100)

        if tasks:
            total = len(tasks)
            completed = sum(1 for t in tasks if t.get("completed", False))
            percentage = round((completed / total * 100) if total > 0 else 0)
        else:
            total = 0
            completed = 0
            percentage = 0

        weekly_data.append({
            "date": day_str,
            "day_abbr": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][day_date.weekday()],
            "total": total,
            "completed": completed,
            "percentage": percentage
        })

    # Calculate average
    days_with_data = [d for d in weekly_data if d["total"] > 0]
    avg_percentage = round(sum(d["percentage"] for d in days_with_data) / len(days_with_data)) if days_with_data else 0

    return {
        "days": weekly_data,
        "average_percentage": avg_percentage,
        "total_completed": sum(d["completed"] for d in weekly_data),
        "total_tasks": sum(d["total"] for d in weekly_data)
    }


# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



def _parse_client_today(today: Optional[str]):
    """The caller's local date, falling back to server UTC. The frontend passes
    its own calendar date so day boundaries follow the user's timezone, not
    the server's."""
    if today:
        try:
            return date.fromisoformat(today)
        except ValueError:
            pass
    return datetime.utcnow().date()


@app.get("/api/history")
async def get_history(days: int = 7, today: Optional[str] = None):
    records = []
    base = _parse_client_today(today)

    for i in range(days):
        d = base - timedelta(days=i)
        date_str = d.isoformat()

        tasks = await db.daily_tasks.find({"date": date_str}).to_list(None)
        total = len(tasks)
        done  = sum(1 for t in tasks if t.get("completed", False))
        pct   = round((done / total * 100), 1) if total > 0 else 0.0

        records.append({
            "date":  date_str,
            "done":  done,
            "total": total,
            "pct":   pct,
        })

    return records


@app.get("/api/streak")
async def get_streak(today: Optional[str] = None):
    streak = 0
    base = _parse_client_today(today)

    for i in range(365):
        d = base - timedelta(days=i)
        date_str = d.isoformat()

        tasks = await db.daily_tasks.find({"date": date_str}).to_list(None)
        if not tasks:
            continue

        total = len(tasks)
        done  = sum(1 for t in tasks if t.get("completed", False))

        if done < total:
            break

        streak += 1

    return {"streak": streak}


@app.get("/api/analytics/trends")
async def get_analytics_trends(
    range: int = 30,
    client_now: Optional[str] = None,
    tz_offset_minutes: int = 0,
):
    """Three independent diagnostics — Follow-Through, Lost Time by
    Activity, Time of Day Performance — plus one final combined
    recommendation, for the History Trends tab. Each graph's diagnostic
    logic lives in analytics.py and returns exactly one key from its own
    fixed set; a separate combination engine (also in analytics.py) turns
    those three diagnostics into the single suggestion shown at the bottom.

    client_now is the caller's local timestamp (YYYY-MM-DDTHH:MM[:SS]) —
    both the day boundary and, for today specifically, whether a still-open
    task's scheduled window has already closed depend on it. Without it we
    fall back to server UTC, which will misjudge "today" for users behind
    or ahead of UTC exactly like every other client_today fallback in this
    file — callers should always pass it."""
    range_days = range if range in (7, 30, 90) else 30

    if client_now:
        try:
            now_dt = datetime.fromisoformat(client_now)
        except ValueError:
            now_dt = datetime.utcnow()
    else:
        now_dt = datetime.utcnow()

    client_today = now_dt.date().isoformat()
    now_minutes = now_dt.hour * 60 + now_dt.minute

    range_start = (now_dt.date() - timedelta(days=range_days - 1)).isoformat()
    raw_tasks = await db.daily_tasks.find(
        {"date": {"$gte": range_start, "$lte": client_today}}
    ).to_list(100000)

    tasks = [analytics.normalize_task(t, client_today, now_minutes) for t in raw_tasks]

    display_buckets = analytics.build_display_buckets(tasks, range_days, client_today)
    for b in display_buckets:
        b["metrics"] = analytics.compute_metrics(b["tasks"], tz_offset_minutes)
        del b["tasks"]
    heavy_pattern = analytics.compute_heavy_day_pattern(tasks)

    ft_diag = analytics.follow_through_diagnosis(tasks, range_days, client_today, display_buckets, heavy_pattern)
    follow_through = {"buckets": display_buckets, "diagnostic": ft_diag}

    lost_time = analytics.lost_time_diagnosis(tasks, tz_offset_minutes)

    # Narrow, deliberate exception to "never touch live schedule_slots for
    # analytics": Time of Day Performance needs to know what time the
    # selected activity is scheduled at *right now* to compare current vs.
    # alternative periods. This never changes any historical task record —
    # it's a one-field lookup purely to answer "where does this sit today."
    current_slot_start_time = None
    if lost_time["selected"]:
        current_slot = await db.schedule_slots.find_one({"id": lost_time["selected"]["slot_id"]})
        if current_slot:
            current_slot_start_time = current_slot.get("start_time")

    time_of_day = analytics.time_of_day_diagnosis(
        tasks, lost_time["selected"], current_slot_start_time, tz_offset_minutes
    )

    recommendation = analytics.combine_diagnostics(ft_diag, lost_time, time_of_day, range_days)

    return {
        "range": range_days,
        "follow_through": follow_through,
        "lost_time": lost_time,
        "time_of_day": time_of_day,
        "recommendation": recommendation,
    }


@app.get("/api/export")
async def export_data():
    slots = await db.schedule_slots.find().sort("order_index", 1).to_list(1000)
    tasks = await db.daily_tasks.find().sort("date", 1).to_list(1000000)

    for doc in slots:
        doc.pop("_id", None)
    for doc in tasks:
        doc.pop("_id", None)

    return {
        "exported_at": datetime.utcnow().isoformat(),
        "schedule_slots": slots,
        "daily_tasks": tasks,
    }


@app.post("/api/import")
async def import_data(payload: ImportData):
    """Restore schedule_slots and daily_tasks from a previously exported backup.
    This replaces all current data (matches the 'restore after reset' use case)."""
    slots = payload.schedule_slots
    tasks = payload.daily_tasks

    for s in slots:
        if not all(k in s for k in ("id", "label", "start_time", "end_time", "order_index")):
            raise HTTPException(status_code=400, detail="Invalid schedule_slots entry in backup file")
    for t in tasks:
        if not all(k in t for k in ("id", "date", "slot_id")):
            raise HTTPException(status_code=400, detail="Invalid daily_tasks entry in backup file")

    await db.schedule_slots.delete_many({})
    await db.daily_tasks.delete_many({})

    if slots:
        await db.schedule_slots.insert_many(slots)
    if tasks:
        await db.daily_tasks.insert_many(tasks)

    # Backfill name/start_time/end_time/duration in case this is an older
    # export taken before daily_tasks stored those fields directly.
    await _backfill_daily_task_names(db)

    return {
        "schedule_slots_imported": len(slots),
        "daily_tasks_imported": len(tasks),
    }


@app.delete("/api/reset")
async def reset_all_data():
    """Wipes everything: all daily task history/completions AND the Routine
    template itself, leaving a genuinely empty app (no activities, so
    Today and History have nothing to generate tasks from either)."""
    tasks_result = await db.daily_tasks.delete_many({})
    slots_result = await db.schedule_slots.delete_many({})

    return {
        "deleted_tasks": tasks_result.deleted_count,
        "deleted_slots": slots_result.deleted_count,
    }


@app.delete("/api/reset/today")
async def reset_today_tasks(client_today: Optional[str] = None):
    """Deletes only current/future daily_tasks (today onward), leaving every
    past-dated task (History, Trends) and the Routine template untouched.

    Also records which activities existed in Routine at this exact moment,
    so get_daily_tasks above knows specifically which ones not to bring
    back — without this, viewing Today would immediately regenerate every
    one of them right back, since that view is itself the read that
    triggers lazy regeneration. Anything you add to Routine after clearing
    is a new slot_id and isn't in this snapshot, so it still shows up
    normally; the marker only ever matches one exact date string, so
    tomorrow (or any date you view before clearing again) is unaffected."""
    today = client_today or datetime.utcnow().date().isoformat()
    result = await db.daily_tasks.delete_many({"date": {"$gte": today}})
    existing_slots = await db.schedule_slots.find({}, {"id": 1}).to_list(1000)
    await db.app_meta.update_one(
        {"key": "cleared_today_date"},
        {"$set": {
            "key": "cleared_today_date",
            "value": today,
            "slot_ids": [s["id"] for s in existing_slots],
        }},
        upsert=True,
    )
    return {"deleted_tasks": result.deleted_count}


@app.delete("/api/reset/routine")
async def reset_routine_only():
    """Deletes only the Routine template (schedule_slots). All daily_tasks
    history is untouched — past records already carry their own baked-in
    name/start_time/end_time/duration (see _backfill_daily_task_names), so
    History and Trends stay accurate even once the originating activity no
    longer exists in Routine."""
    result = await db.schedule_slots.delete_many({})
    return {"deleted_slots": result.deleted_count}
