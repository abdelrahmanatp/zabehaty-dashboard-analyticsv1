# Workflow: User Analysis (RFM + LTV)

## Objective
Score all users with RFM (Recency/Frequency/Monetary) segmentation and compute historical + predictive LTV.

## Primary Data Source
- `user_total_orders` — cumulative per-user per-category spend (2021–2026). **This is the main history table.** `orders_before_2023` is empty; `orders` only covers recent weeks.

## Tool
`tools/user_analysis.py`

## Outputs
| File | Contents |
|------|----------|
| `.tmp/rfm_scores.csv` | Per-user RFM scores, segment label, recency, monetary |
| `.tmp/ltv_analysis.csv` | Historical LTV + predicted 12-month LTV per user |
| `.tmp/user_segments.json` | Segment summary stats (count, avg LTV, revenue share) |
| `.tmp/cohort_retention.csv` | Monthly cohort retention matrix |

## Segment Labels
| Segment | Condition |
|---------|-----------|
| Champions | R≥4, F≥4, M≥4 |
| Loyal Customers | R≥3, M≥4 |
| New Customers | R≥4, F≤2 |
| At Risk | R≤2, F≥3, M≥3 |
| Cant Lose Them | R≤2, F≥4, M≥4 |
| Lost | R≤2, F≤2 |
| Need Attention | R=3, F≤2 |
| About to Sleep | Remaining |

## Known Constraints
- Frequency proxy = categories_purchased (not order count, since order count history is in `user_total_orders` aggregated form)
- Recency uses `orders.created_at` for recent users; falls back to `user_total_orders.updated_at`
- Users with `is_ban=1` are excluded

## Edge Cases
- Users with NULL last_order_date → treated as highest recency (worst R score)
- Predictive LTV uses sigmoid decay based on 180-day half-life churn assumption
