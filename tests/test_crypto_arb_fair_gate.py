from polybot.crypto.fair_price import FairPriceSnapshot
from polybot.live.arb_sniper import ArbPlan, fair_model_accepts_arb_plan


def plan(yes_avg: float, no_avg: float) -> ArbPlan:
    return ArbPlan(
        yes_size=10.0,
        no_size=10.0,
        size=10.0,
        yes_limit=yes_avg,
        no_limit=no_avg,
        yes_cost_est=yes_avg * 10.0,
        no_cost_est=no_avg * 10.0,
        total_cost_est=(yes_avg + no_avg) * 10.0,
        avg_sum_est=yes_avg + no_avg,
        edge_per_pair=1.0 - yes_avg - no_avg,
        first_leg="YES",
        second_leg="NO",
    )


def snap(fair_up: float) -> FairPriceSnapshot:
    return FairPriceSnapshot(
        fair_up=fair_up,
        fair_down=1.0 - fair_up,
        sigma_annualized=1.0,
        z_score=0.0,
        seconds_to_expiry=60.0,
        start_price=100.0,
        current_price=100.0,
    )


def test_fair_model_gate_accepts_pair_when_both_legs_clear_model_edge():
    assert fair_model_accepts_arb_plan(plan(0.44, 0.53), snap(0.45), min_model_edge=0.005)


def test_fair_model_gate_rejects_complement_pair_when_one_leg_overpays_model():
    assert not fair_model_accepts_arb_plan(plan(0.50, 0.47), snap(0.45), min_model_edge=0.005)


def test_fair_model_gate_can_be_disabled():
    assert fair_model_accepts_arb_plan(plan(0.50, 0.47), None, min_model_edge=0.005)
