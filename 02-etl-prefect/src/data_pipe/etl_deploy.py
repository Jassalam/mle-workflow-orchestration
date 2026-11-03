from datetime import UTC, datetime, timedelta

from etl_pipeline import main_flow
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
    main_flow.serve(
        name="ETL Pipeline",
        schedules=[
            schedule_rule,
        ],
        parameters={"color": "yellow", "month": 1},
        tags=["etl"],
    )
