"""Excerpt from a reporting service focused on aggregation-heavy admin reporting.

Selected sections show timezone-aware bucketing helpers plus representative
income and payment reliability report queries.
"""
"""Service layer for admin report aggregation queries."""

from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import and_, case, func, literal_column, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from app.models.camp_program import CampProgram
from app.models.payment import Payment, PaymentStatus, PaymentType
from app.models.program_day import ProgramDay
from app.models.program_week import ProgramWeek
from app.models.registration import Registration, RegistrationStatus
from app.models.registration_modification_log import (
    ModificationType,
    RegistrationModificationLog,
)
from app.models.scheduled_payment import ScheduledPayment, ScheduledPaymentStatus
from app.models.third_party_contribution import ThirdPartyContribution
from app.models.waitlist import WaitlistEntry, WaitlistStatus
from app.schemas.reports import (
    CancellationBreakdown,
    CapacityPreviousPeriod,
    CapacityUtilization,
    ContributionBySource,
    DailyCapacityRow,
    DailySeriesPoint,
    DiscountsContributions,
    DiscountsPreviousPeriod,
    FailureReason,
    IncomeOverview,
    IncomePreviousPeriod,
    PaymentReliability,
    PaymentReliabilityPreviousPeriod,
    RegistrationDailyPoint,
    RegistrationsChurn,
    RegistrationsPreviousPeriod,
    ReportSummary,
    StatusBreakdown,
    TuitionPipeline,
    TuitionPipelinePreviousPeriod,
    WeeklyForecast,
)
from app.services.base import OrgScopedService

ZERO = Decimal("0.0")


class ReportService(OrgScopedService):
    """Aggregation service for admin report endpoints.

    Extends OrgScopedService with timezone awareness for date bucketing.
    """

    def __init__(self, db: AsyncSession, org_id: int, tz: str) -> None:
        super().__init__(db, org_id)
        self.tz = tz

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _previous_period(start: date, end: date) -> tuple[date, date]:
        """Return (prev_start, prev_end) of equal length preceding *start*."""
        length = (end - start).days + 1
        prev_end = start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=length - 1)
        return prev_start, prev_end

    @staticmethod
    def _current_camp_week(ref: date) -> tuple[date, date]:
        """Return (monday, friday) of the camp week containing *ref*."""
        monday = ref - timedelta(days=ref.weekday())
        friday = monday + timedelta(days=4)
        return monday, friday

    def _base_program_filter(self, stmt: Select[Any]) -> Select[Any]:
        """Add is_active=True, is_archived=False to a CampProgram-joined query."""
        return stmt.where(
            CampProgram.is_active.is_(True),
            CampProgram.is_archived.is_(False),
        )

    def _tz_date(self, col: Any) -> Any:
        """Timezone-aware date bucket: ``func.date(func.timezone(tz, col))``."""
        return func.date(func.timezone(self.tz, col))

    def _date_range_filter(self, col: Any, start: date, end: date) -> Any:
        """Filter a tz-aware timestamp column to [start, end] in org timezone."""
        return and_(
            self._tz_date(col) >= start,
            self._tz_date(col) <= end,
        )

    # ── Income ────────────────────────────────────────────────────────────

    async def get_income_overview(
        self,
        start: date,
        end: date,
        program_id: int | None = None,
        week_id: int | None = None,
        payment_status: PaymentStatus | None = None,
    ) -> IncomeOverview:
        # Resolve payment status filter (default: COMPLETED only)
        ps_filter = (
            Payment.status == payment_status
            if payment_status
            else Payment.status == PaymentStatus.succeeded
        )

        # -- Completed payments in range (gross collected) --
        base = select(
            func.coalesce(func.sum(Payment.amount), ZERO).label("total"),
            func.coalesce(
                func.sum(
                    case(
                        (
                            Payment.payment_type == PaymentType.DEPOSIT,
                            Payment.amount,
                        ),
                        else_=literal_column("0"),
                    )
                ),
                ZERO,
            ).label("deposit"),
            func.coalesce(
                func.sum(
                    case(
                        (
                            Payment.payment_type == PaymentType.TUITION,
                            Payment.amount,
                        ),
                        else_=literal_column("0"),
                    )
                ),
                ZERO,
            ).label("tuition"),
            func.coalesce(
                func.sum(
                    case(
                        (
                            Payment.payment_type == PaymentType.FEE,
                            Payment.amount,
                        ),
                        else_=literal_column("0"),
                    )
                ),
                ZERO,
            ).label("fee"),
        ).where(
            Payment.org_id == self.org_id,
            ps_filter,
            self._date_range_filter(Payment.created_at, start, end),
        )
        if program_id or week_id:
            base = base.join(Registration, Registration.id == Payment.registration_id)
            if program_id:
                base = base.where(Registration.program_id == program_id)
            if week_id:
                base = base.where(Registration.program_week_id == week_id)

        result = await self.db.execute(base)
        row = result.one()
        gross = Decimal(str(row.total))
        deposit_collected = Decimal(str(row.deposit))
        tuition_collected = Decimal(str(row.tuition))
        fee_collected = Decimal(str(row.fee))

        # -- Pending pipeline (pending/processing payments) --
        # When payment_status filter is active, only show pipeline for
        # that status; otherwise default to PENDING + PROCESSING.
        pending_statuses = (
            [payment_status]
            if payment_status
            else [
                PaymentStatus.requires_payment_method,
                PaymentStatus.requires_confirmation,
                PaymentStatus.requires_action,
                PaymentStatus.processing,
            ]
        )
        pending_q = select(func.coalesce(func.sum(Payment.amount), ZERO)).where(
            Payment.org_id == self.org_id,
            Payment.status.in_(pending_statuses),
            self._date_range_filter(Payment.created_at, start, end),
        )
        if program_id or week_id:
            pending_q = pending_q.join(
                Registration, Registration.id == Payment.registration_id
            )
            if program_id:
                pending_q = pending_q.where(Registration.program_id == program_id)
            if week_id:
                pending_q = pending_q.where(Registration.program_week_id == week_id)
        pending_pipeline = Decimal(str((await self.db.execute(pending_q)).scalar_one()))

        # -- Refunds --
        refund_q = select(func.coalesce(func.sum(Payment.refund_amount), ZERO)).where(
            Payment.org_id == self.org_id,
            Payment.refund_amount.is_not(None),
            self._date_range_filter(Payment.created_at, start, end),
        )
        if program_id or week_id:
            refund_q = refund_q.join(
                Registration, Registration.id == Payment.registration_id
            )
            if program_id:
                refund_q = refund_q.where(Registration.program_id == program_id)
            if week_id:
                refund_q = refund_q.where(Registration.program_week_id == week_id)
        total_refunds = Decimal(str((await self.db.execute(refund_q)).scalar_one()))

        # -- Discounts on registrations --
        disc_q = select(
            func.coalesce(func.sum(Registration.discount_amount), ZERO)
        ).where(
            Registration.org_id == self.org_id,
            Registration.status.in_(
                [RegistrationStatus.CONFIRMED, RegistrationStatus.COMPLETED]
            ),
            self._date_range_filter(Registration.created_at, start, end),
        )
        if program_id:
            disc_q = disc_q.where(Registration.program_id == program_id)
        if week_id:
            disc_q = disc_q.where(Registration.program_week_id == week_id)
        total_discounts = Decimal(str((await self.db.execute(disc_q)).scalar_one()))

        # -- Third-party contributions --
        tpc_q = select(
            func.coalesce(func.sum(ThirdPartyContribution.amount), ZERO)
        ).where(
            ThirdPartyContribution.org_id == self.org_id,
            self._date_range_filter(ThirdPartyContribution.created_at, start, end),
        )
        if program_id:
            tpc_q = tpc_q.where(ThirdPartyContribution.program_id == program_id)
        if week_id:
            tpc_q = tpc_q.where(ThirdPartyContribution.program_week_id == week_id)
        third_party = Decimal(str((await self.db.execute(tpc_q)).scalar_one()))

        net_revenue = gross - total_refunds

        # -- Daily series --
        daily_q = (
            select(
                self._tz_date(Payment.created_at).label("d"),
                func.coalesce(func.sum(Payment.amount), ZERO).label("amt"),
            )
            .where(
                Payment.org_id == self.org_id,
                ps_filter,
                self._date_range_filter(Payment.created_at, start, end),
            )
            .group_by("d")
            .order_by("d")
        )
        daily_rows = (await self.db.execute(daily_q)).all()
        daily_series = [
            DailySeriesPoint(date=r.d, amount=Decimal(str(r.amt))) for r in daily_rows
        ]

        # -- Previous period --
        ps, pe = self._previous_period(start, end)
        prev_gross_q = select(func.coalesce(func.sum(Payment.amount), ZERO)).where(
            Payment.org_id == self.org_id,
            ps_filter,
            self._date_range_filter(Payment.created_at, ps, pe),
        )
        if program_id or week_id:
            prev_gross_q = prev_gross_q.join(
                Registration, Registration.id == Payment.registration_id
            )
            if program_id:
                prev_gross_q = prev_gross_q.where(Registration.program_id == program_id)
            if week_id:
                prev_gross_q = prev_gross_q.where(
                    Registration.program_week_id == week_id
                )
        prev_gross = Decimal(str((await self.db.execute(prev_gross_q)).scalar_one()))
        prev_refund_q = select(
            func.coalesce(func.sum(Payment.refund_amount), ZERO)
        ).where(
            Payment.org_id == self.org_id,
            Payment.refund_amount.is_not(None),
            self._date_range_filter(Payment.created_at, ps, pe),
        )
        if program_id or week_id:
            prev_refund_q = prev_refund_q.join(
                Registration, Registration.id == Payment.registration_id
            )
            if program_id:
                prev_refund_q = prev_refund_q.where(
                    Registration.program_id == program_id
                )
            if week_id:
                prev_refund_q = prev_refund_q.where(
                    Registration.program_week_id == week_id
                )
        prev_refunds = Decimal(str((await self.db.execute(prev_refund_q)).scalar_one()))

        return IncomeOverview(
            gross_collected=gross,
            pending_pipeline=pending_pipeline,
            total_refunds=total_refunds,
            total_discounts=total_discounts,
            third_party_contributions=third_party,
            net_revenue=net_revenue,
            deposit_collected=deposit_collected,
            tuition_collected=tuition_collected,
            fee_collected=fee_collected,
            daily_series=daily_series,
            previous_period=IncomePreviousPeriod(
                start_date=ps,
                end_date=pe,
                gross_collected=prev_gross,
                net_revenue=prev_gross - prev_refunds,
            ),
        )

    # ── Tuition Pipeline ─────────────────────────────────────────────────



# ... other report modules omitted for brevity ...

    async def get_payment_reliability(
        self,
        start: date,
        end: date,
        program_id: int | None = None,
        week_id: int | None = None,
        scheduled_payment_status: ScheduledPaymentStatus | None = None,
    ) -> PaymentReliability:
        sp_status = scheduled_payment_status

        def _base_sp_select(*cols: Any) -> Select[Any]:
            q = select(*cols).where(
                ScheduledPayment.org_id == self.org_id,
                ScheduledPayment.scheduled_date >= start,
                ScheduledPayment.scheduled_date <= end,
            )
            if sp_status is not None:
                q = q.where(ScheduledPayment.status == sp_status)
            if program_id:
                q = q.join(
                    Registration,
                    Registration.batch_id == ScheduledPayment.batch_id,
                ).where(Registration.program_id == program_id)
                if week_id:
                    q = q.where(Registration.program_week_id == week_id)
            return q

        # Total processed (completed + failed)
        total_q = _base_sp_select(func.count()).where(
            ScheduledPayment.status.in_(
                [
                    ScheduledPaymentStatus.COMPLETED,
                    ScheduledPaymentStatus.FAILED,
                ]
            )
        )
        total_processed = (await self.db.execute(total_q)).scalar_one() or 0

        failed_q = _base_sp_select(func.count()).where(
            ScheduledPayment.status == ScheduledPaymentStatus.FAILED,
        )
        total_failed = (await self.db.execute(failed_q)).scalar_one() or 0
        failure_rate = (
            Decimal(str(round(total_failed / total_processed * 100, 1)))
            if total_processed > 0
            else ZERO
        )

        # Auto retry success: retry_count > 0 AND completed
        auto_retry_total_q = _base_sp_select(func.count()).where(
            ScheduledPayment.retry_count > 0,
        )
        auto_retry_total = (await self.db.execute(auto_retry_total_q)).scalar_one() or 0
        auto_retry_success_q = _base_sp_select(func.count()).where(
            ScheduledPayment.retry_count > 0,
            ScheduledPayment.status == ScheduledPaymentStatus.COMPLETED,
        )
        auto_retry_success = (
            await self.db.execute(auto_retry_success_q)
        ).scalar_one() or 0
        auto_retry_rate = (
            Decimal(str(round(auto_retry_success / auto_retry_total * 100, 1)))
            if auto_retry_total > 0
            else ZERO
        )

        # Manual retry metrics
        manual_q = _base_sp_select(
            func.count().label("cnt"),
            func.coalesce(func.sum(ScheduledPayment.manual_retry_count), 0).label(
                "cycles"
            ),
        ).where(ScheduledPayment.manual_retry_count > 0)
        manual_row = (await self.db.execute(manual_q)).one()
        payments_with_manual = int(manual_row.cnt)
        total_manual_cycles = int(manual_row.cycles)

        manual_success_q = _base_sp_select(func.count()).where(
            ScheduledPayment.manual_retry_count > 0,
            ScheduledPayment.status == ScheduledPaymentStatus.COMPLETED,
        )
        manual_success = (await self.db.execute(manual_success_q)).scalar_one() or 0
        manual_rate = (
            Decimal(str(round(manual_success / payments_with_manual * 100, 1)))
            if payments_with_manual > 0
            else ZERO
        )

        # Aged unresolved (failed payments by age)
        today = date.today()

        async def _aged_count(days: int) -> int:
            cutoff = today - timedelta(days=days)
            q = _base_sp_select(func.count()).where(
                ScheduledPayment.status == ScheduledPaymentStatus.FAILED,
                ScheduledPayment.scheduled_date <= cutoff,
            )
            return (await self.db.execute(q)).scalar_one() or 0

        aged_7 = await _aged_count(7)
        aged_14 = await _aged_count(14)
        aged_30 = await _aged_count(30)

        # Total unresolved amount
        unresolved_q = _base_sp_select(
            func.coalesce(func.sum(ScheduledPayment.amount), ZERO)
        ).where(ScheduledPayment.status == ScheduledPaymentStatus.FAILED)
        total_unresolved = Decimal(
            str((await self.db.execute(unresolved_q)).scalar_one())
        )

        # Failure reasons
        reasons_q = (
            _base_sp_select(
                func.coalesce(
                    ScheduledPayment.last_error_code,
                    literal_column("'unknown'"),
                ).label("reason"),
                func.count().label("cnt"),
            )
            .where(ScheduledPayment.status == ScheduledPaymentStatus.FAILED)
            .group_by(
                func.coalesce(
                    ScheduledPayment.last_error_code,
                    literal_column("'unknown'"),
                )
            )
            .order_by(func.count().desc())
        )
        reason_rows = (await self.db.execute(reasons_q)).all()
        failure_reasons = [
            FailureReason(reason=r.reason, count=r.cnt) for r in reason_rows
        ]

        # Previous period — apply same program/week filters
        ps, pe = self._previous_period(start, end)

        def _prev_sp_select(*cols: Any) -> Select[Any]:
            q = select(*cols).where(
                ScheduledPayment.org_id == self.org_id,
                ScheduledPayment.scheduled_date >= ps,
                ScheduledPayment.scheduled_date <= pe,
            )
            if program_id:
                q = q.join(
                    Registration,
                    Registration.batch_id == ScheduledPayment.batch_id,
                ).where(Registration.program_id == program_id)
                if week_id:
                    q = q.where(Registration.program_week_id == week_id)
            return q

        prev_total_q = _prev_sp_select(func.count()).where(
            ScheduledPayment.status.in_(
                [
                    ScheduledPaymentStatus.COMPLETED,
                    ScheduledPaymentStatus.FAILED,
                ]
            ),
        )
        prev_total = (await self.db.execute(prev_total_q)).scalar_one() or 0
        prev_failed_q = _prev_sp_select(func.count()).where(
            ScheduledPayment.status == ScheduledPaymentStatus.FAILED,
        )
        prev_failed = (await self.db.execute(prev_failed_q)).scalar_one() or 0
        prev_failure_rate = (
            Decimal(str(round(prev_failed / prev_total * 100, 1)))
            if prev_total > 0
            else ZERO
        )
        prev_unresolved_q = _prev_sp_select(
            func.coalesce(func.sum(ScheduledPayment.amount), ZERO)
        ).where(ScheduledPayment.status == ScheduledPaymentStatus.FAILED)
        prev_unresolved = Decimal(
            str((await self.db.execute(prev_unresolved_q)).scalar_one())
        )

        return PaymentReliability(
            failure_rate_pct=failure_rate,
            auto_retry_success_rate=auto_retry_rate,
            manual_retry_success_rate=manual_rate,
            payments_with_manual_retry=payments_with_manual,
            total_manual_retry_cycles=total_manual_cycles,
            aged_unresolved_7d=aged_7,
            aged_unresolved_14d=aged_14,
            aged_unresolved_30d=aged_30,
            total_unresolved_amount=total_unresolved,
            failure_reasons=failure_reasons,
            previous_period=PaymentReliabilityPreviousPeriod(
                start_date=ps,
                end_date=pe,
                failure_rate_pct=prev_failure_rate,
                total_unresolved_amount=prev_unresolved,
            ),
        )

    # ── Summary ───────────────────────────────────────────────────────────

    async def get_summary(
        self,
        start: date,
        end: date,
        program_id: int | None = None,
        week_id: int | None = None,
        payment_status: PaymentStatus | None = None,
        scheduled_payment_status: ScheduledPaymentStatus | None = None,
    ) -> ReportSummary:
        income = await self.get_income_overview(
            start, end, program_id, week_id, payment_status
        )
        pipeline = await self.get_tuition_pipeline(
            start, end, program_id, week_id, scheduled_payment_status
        )
        capacity = await self.get_capacity_utilization(start, end, program_id, week_id)
        regs = await self.get_registrations_churn(start, end, program_id, week_id)
        discounts = await self.get_discounts_contributions(
            start, end, program_id, week_id, payment_status
        )
        reliability = await self.get_payment_reliability(
            start, end, program_id, week_id, scheduled_payment_status
        )

        return ReportSummary(
            gross_collected=income.gross_collected,
            net_revenue=income.net_revenue,
            current_week_due=pipeline.current_week_due,
            current_week_collected=pipeline.current_week_collected,
            avg_utilization_pct=capacity.avg_utilization_pct,
            total_waitlist_entries=capacity.total_waitlist_entries,
            new_confirmations=regs.new_confirmations,
            cancellations=regs.cancellations,
            total_discount_amount=discounts.total_discount_amount,
            total_third_party_amount=discounts.total_third_party_amount,
            failure_rate_pct=reliability.failure_rate_pct,
            total_unresolved_amount=reliability.total_unresolved_amount,
        )
