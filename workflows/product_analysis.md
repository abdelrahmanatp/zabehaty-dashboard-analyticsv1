# Workflow: Product Analysis (BCG Matrix)

## Objective
Classify all products into BCG quadrants, identify top performers, and generate promote/drop/maintain recommendations.

## Data Sources
- `order_details` JOIN `orders` (status=3, payment_status='completed') — delivered line items
- `products` — catalog with pricing and cost
- `shops` — vendor names
- `categories` — category names

## Tool
`tools/product_analysis.py`

## BCG Classification Logic
Since `orders` only covers ~3 weeks (insufficient for growth-rate time series), BCG uses:
- **X-axis (Market Share):** Product revenue ÷ category total revenue
- **Y-axis (Vitality):** total_orders × avg_margin (demand × profitability composite)
- Quadrant lines: median of each axis

| Quadrant | Share | Vitality | Action |
|----------|-------|----------|--------|
| Star | High | High | Invest, promote |
| Cash Cow | High | Low | Protect margin, minimal spend |
| Question Mark | Low | High | Evaluate, targeted promotion |
| Dog | Low | Low | Drop or restructure |

## Outputs
| File | Contents |
|------|----------|
| `.tmp/bcg_matrix.csv` | Per-product BCG quadrant + metrics |
| `.tmp/top_products.csv` | Top 50 by revenue |
| `.tmp/product_recommendations.json` | Promote/maintain/drop/never-sold lists |
| `.tmp/category_performance.csv` | Revenue, margin, order count by category |

## Known Constraints
- `order_details.cost_price` is often NULL → margin calculation may understate profit
- Products with `deleted_at IS NOT NULL` are excluded from catalog analysis
- BCG vitality proxy will be replaced with true growth rate once 90+ days of orders accumulate
