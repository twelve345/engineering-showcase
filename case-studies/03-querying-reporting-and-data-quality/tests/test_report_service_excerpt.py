"""Excerpt of reporting tests focused on correctness of representative metrics.

Selected tests cover income aggregation and payment reliability calculations.
"""
"""AC2 + AC5: Aggregation correctness for all 6 report modules + summary.

Instantiates ReportService(db_session, org_id, tz) directly.
Uses local fixtures (separate from api/conftest.py) for exact data control.
"""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Organization
from app.models.camp_program import CampProgram
from app.models.child import Child
from app.models.payment import Payment, PaymentMethod, PaymentStatus, PaymentType
from app.models.program_day import ProgramDay
from app.models.program_week import ProgramWeek
from app.models.registration import Registration, RegistrationStatus
from app.models.registration_modification_log import (
    ModificationType,
    RegistrationModificationLog,
)
from app.models.scheduled_payment import ScheduledPayment, ScheduledPaymentStatus
from app.models.third_party_contribution import ThirdPartyContribution
from app.models.user import User
from app.models.waitlist import WaitlistEntry, WaitlistStatus
from app.services.report_service import ReportService

TEST_CAMP_WEEK_MONDAY = date(2025, 6, 2)
TEST_CAMP_WEEK_FRIDAY = TEST_CAMP_WEEK_MONDAY + timedelta(days=4)
# Noon UTC on Monday — safely within America/Chicago date for the entire week
TEST_SAFE_TS = datetime(2025, 6, 2, 17, 0, tzinfo=UTC)


def _current_camp_week() -> tuple[date, date]:
    """Return a deterministic [monday, friday] camp week for tests."""
    return TEST_CAMP_WEEK_MONDAY, TEST_CAMP_WEEK_FRIDAY


# ── Local fixtures (not shared with api tests) ────────────────────────


@pytest_asyncio.fixture
async def svc_program(db_session: AsyncSession, test_org: Organization) -> CampProgram:
    monday, friday = _current_camp_week()
    prog = CampProgram(
        org_id=test_org.id,
        name="SVC Test Program",
        season_year=2025,
        timezone="America/Chicago",
        start_date=monday,
        end_date=friday + timedelta(weeks=8),
        start_time=datetime.strptime("08:00", "%H:%M").time(),
        end_time=datetime.strptime("16:00", "%H:%M").time(),
        max_capacity=20,
        is_active=True,
        is_archived=False,
    )
    db_session.add(prog)
    await db_session.flush()
    return prog


@pytest_asyncio.fixture
async def svc_week(
    db_session: AsyncSession,
    test_org: Organization,
    svc_program: CampProgram,
) -> ProgramWeek:
    monday, friday = _current_camp_week()
    week = ProgramWeek(
        org_id=test_org.id,
        program_id=svc_program.id,
        week_number=1,
        week_start_date=monday,
        week_end_date=friday,
        timezone="America/Chicago",
    )
    db_session.add(week)
    await db_session.flush()
    return week


@pytest_asyncio.fixture
async def svc_days(
    db_session: AsyncSession,
    test_org: Organization,
    svc_program: CampProgram,
    svc_week: ProgramWeek,
) -> list[ProgramDay]:
    monday, _friday = _current_camp_week()
    days = []
    for i in range(5):
        day = ProgramDay(
            org_id=test_org.id,
            program_id=svc_program.id,
            program_week_id=svc_week.id,
            date=monday + timedelta(days=i),
            is_active=True,
        )
        db_session.add(day)
        days.append(day)
    await db_session.flush()
    return days


@pytest_asyncio.fixture
async def svc_child(
    db_session: AsyncSession, test_org: Organization, test_user: User
) -> Child:
    child = Child(
        org_id=test_org.id,
        parent_id=test_user.id,
        first_name="SVC",
        last_name="Kid",
        date_of_birth=date(2017, 1, 1),
        coppa_consent_given=True,
    )
    db_session.add(child)
    await db_session.flush()
    return child


@pytest_asyncio.fixture
async def svc_service(
    db_session: AsyncSession, test_org: Organization
) -> ReportService:
    return ReportService(db_session, test_org.id, "America/Chicago")


# ── TestIncomeOverview ─────────────────────────────────────────────────


class TestIncomeOverview:
    @pytest_asyncio.fixture
    async def income_data(
        self,
        db_session: AsyncSession,
        test_org: Organization,
        test_user: User,
        svc_program: CampProgram,
        svc_week: ProgramWeek,
        svc_days: list[ProgramDay],
        svc_child: Child,
    ) -> list[Payment]:
        now = TEST_SAFE_TS
        reg = Registration(
            org_id=test_org.id,
            child_id=svc_child.id,
            program_id=svc_program.id,
            program_day_id=svc_days[0].id,
            program_week_id=svc_week.id,
            parent_id=test_user.id,
            status=RegistrationStatus.CONFIRMED,
            price_paid=Decimal("400.00"),
            discount_amount=Decimal("25.00"),
            batch_id="svc-inc-001",
            created_at=now,
        )
        db_session.add(reg)
        await db_session.flush()

        tpc = ThirdPartyContribution(
            org_id=test_org.id,
            child_id=svc_child.id,
            program_id=svc_program.id,
            program_week_id=svc_week.id,
            created_by_user_id=test_user.id,
            amount=Decimal("100.00"),
            source="Grant",
            season_year=2025,
            created_at=now,
        )
        db_session.add(tpc)

        payments = [
            Payment(
                org_id=test_org.id,
                user_id=test_user.id,
                registration_id=reg.id,
                amount=Decimal("30.00"),
                status=PaymentStatus.succeeded,
                payment_type=PaymentType.DEPOSIT,
                method=PaymentMethod.CREDIT_CARD,
                created_at=now,
            ),
            Payment(
                org_id=test_org.id,
                user_id=test_user.id,
                registration_id=reg.id,
                amount=Decimal("350.00"),
                status=PaymentStatus.succeeded,
                payment_type=PaymentType.TUITION,
                method=PaymentMethod.CREDIT_CARD,
                created_at=now,
            ),
            Payment(
                org_id=test_org.id,
                user_id=test_user.id,
                registration_id=reg.id,
                amount=Decimal("20.00"),
                status=PaymentStatus.succeeded,
                payment_type=PaymentType.FEE,
                method=PaymentMethod.CREDIT_CARD,
                refund_amount=Decimal("10.00"),
                created_at=now,
            ),
            Payment(
                org_id=test_org.id,
                user_id=test_user.id,
                registration_id=reg.id,
                amount=Decimal("200.00"),
                status=PaymentStatus.requires_payment_method,
                payment_type=PaymentType.TUITION,
                method=PaymentMethod.CREDIT_CARD,
                created_at=now,
            ),
        ]
        db_session.add_all(payments)
        await db_session.flush()
        return payments

    @pytest.mark.asyncio(loop_scope="session")
    async def test_gross_collected(
        self, svc_service: ReportService, income_data: list[Payment]
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_income_overview(monday, friday)
        # 30 + 350 + 20 = 400 (only COMPLETED)
        assert result.gross_collected == Decimal("400.00")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_type_breakdown(
        self, svc_service: ReportService, income_data: list[Payment]
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_income_overview(monday, friday)
        assert result.deposit_collected == Decimal("30.00")
        assert result.tuition_collected == Decimal("350.00")
        assert result.fee_collected == Decimal("20.00")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_pending_pipeline(
        self, svc_service: ReportService, income_data: list[Payment]
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_income_overview(monday, friday)
        assert result.pending_pipeline == Decimal("200.00")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_refunds(
        self, svc_service: ReportService, income_data: list[Payment]
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_income_overview(monday, friday)
        assert result.total_refunds == Decimal("10.00")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_discounts(
        self, svc_service: ReportService, income_data: list[Payment]
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_income_overview(monday, friday)
        assert result.total_discounts == Decimal("25.00")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_third_party(
        self, svc_service: ReportService, income_data: list[Payment]
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_income_overview(monday, friday)
        assert result.third_party_contributions == Decimal("100.00")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_net_revenue(
        self, svc_service: ReportService, income_data: list[Payment]
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_income_overview(monday, friday)
        # net = gross - refunds = 400 - 10 = 390
        assert result.net_revenue == Decimal("390.00")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_daily_series_populated(
        self, svc_service: ReportService, income_data: list[Payment]
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_income_overview(monday, friday)
        assert len(result.daily_series) >= 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_previous_period_dates(
        self, svc_service: ReportService, income_data: list[Payment]
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_income_overview(monday, friday)
        pp = result.previous_period
        length = (friday - monday).days + 1
        assert (pp.end_date - pp.start_date).days + 1 == length
        assert pp.end_date == monday - timedelta(days=1)

    @pytest.mark.asyncio(loop_scope="session")
    async def test_empty_data_zeros(
        self,
        db_session: AsyncSession,
        test_org: Organization,
    ):
        """No payments → all zeros."""
        svc = ReportService(db_session, test_org.id, "America/Chicago")
        far_start = date(2020, 1, 1)
        far_end = date(2020, 1, 7)
        result = await svc.get_income_overview(far_start, far_end)
        assert result.gross_collected == Decimal("0.00")
        assert result.net_revenue == Decimal("0.00")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_valid_payment_status_filter(
        self, svc_service: ReportService, income_data: list[Payment]
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_income_overview(
            monday, friday, payment_status=PaymentStatus.succeeded
        )
        assert result.gross_collected >= Decimal("0.00")


# ── TestTuitionPipeline ────────────────────────────────────────────────




# ... other report test modules omitted for brevity ...

class TestPaymentReliability:
    @pytest_asyncio.fixture
    async def reliability_data(
        self,
        db_session: AsyncSession,
        test_org: Organization,
        test_user: User,
    ) -> list[ScheduledPayment]:
        monday, friday = _current_camp_week()
        sps = [
            ScheduledPayment(
                org_id=test_org.id,
                user_id=test_user.id,
                amount=Decimal("100.00"),
                scheduled_date=monday,
                status=ScheduledPaymentStatus.COMPLETED,
                batch_id="svc-rel-001",
                retry_count=2,
            ),
            ScheduledPayment(
                org_id=test_org.id,
                user_id=test_user.id,
                amount=Decimal("75.00"),
                scheduled_date=monday + timedelta(days=1),
                status=ScheduledPaymentStatus.FAILED,
                batch_id="svc-rel-001",
                last_error_code="card_declined",
                manual_retry_count=1,
            ),
            ScheduledPayment(
                org_id=test_org.id,
                user_id=test_user.id,
                amount=Decimal("50.00"),
                scheduled_date=friday,
                status=ScheduledPaymentStatus.COMPLETED,
                batch_id="svc-rel-001",
            ),
        ]
        db_session.add_all(sps)
        await db_session.flush()
        return sps

    @pytest.mark.asyncio(loop_scope="session")
    async def test_failure_rate(
        self, svc_service: ReportService, reliability_data: list
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_payment_reliability(monday, friday)
        # 1 failed / 3 processed = 33.3%
        assert result.failure_rate_pct == Decimal("33.3")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_auto_retry_success_rate(
        self, svc_service: ReportService, reliability_data: list
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_payment_reliability(monday, friday)
        # 1 with retry_count>0 AND completed / 1 with retry_count>0 = 100%
        assert result.auto_retry_success_rate == Decimal("100.0")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_manual_retry_metrics(
        self, svc_service: ReportService, reliability_data: list
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_payment_reliability(monday, friday)
        assert result.payments_with_manual_retry == 1
        assert result.total_manual_retry_cycles == 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_unresolved_amount(
        self, svc_service: ReportService, reliability_data: list
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_payment_reliability(monday, friday)
        assert result.total_unresolved_amount == Decimal("75.00")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_failure_reasons(
        self, svc_service: ReportService, reliability_data: list
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_payment_reliability(monday, friday)
        assert len(result.failure_reasons) == 1
        assert result.failure_reasons[0].reason == "card_declined"
        assert result.failure_reasons[0].count == 1

    @pytest.mark.asyncio(loop_scope="session")
    async def test_scheduled_payment_status_filter(
        self, svc_service: ReportService, reliability_data: list
    ):
        monday, friday = _current_camp_week()
        result = await svc_service.get_payment_reliability(
            monday, friday, scheduled_payment_status=ScheduledPaymentStatus.COMPLETED
        )
        # Only 2 COMPLETED payments visible; 0 failures
        assert result.failure_rate_pct == Decimal("0")
        assert result.total_unresolved_amount == Decimal("0")
        assert len(result.failure_reasons) == 0


# ── TestSummary ────────────────────────────────────────────────────────


