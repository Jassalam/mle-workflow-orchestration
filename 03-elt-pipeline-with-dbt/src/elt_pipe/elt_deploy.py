from datetime import UTC, datetime, timedelta

from elt_pipeline import elt_flow
from prefect.client.schemas.schedules import RRuleSchedule

# Start the first run 2 minutes from now, then every 2 months on the same day and time
now = datetime.now(UTC) + timedelta(minutes=2)
rrule_string = (
    f"DTSTART:{now.strftime('%Y%m%dT%H%M%SZ')}\n"
    f"FREQ=MONTHLY;INTERVAL=2;BYMONTHDAY={now.day};BYHOUR={now.hour};BYMINUTE={now.minute}"
)

schedule_rule = RRuleSchedule(rrule=rrule_string, timezone="UTC")

if __name__ == "__main__":
    # Start the flow with a schedule
    elt_flow.serve(
        name="ELT Pipeline",
        schedules=[
            schedule_rule,
        ],
        parameters={"color": "yellow", "month": 1},
        tags=["elt"],
    )
