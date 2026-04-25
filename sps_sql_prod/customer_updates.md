# SPS Supplier Segmentation: Customer-Facing Updates

**Last Updated:** April 25, 2026

---

## Section 1: Segmentation Framework
SPS classifies suppliers into four tiers based on two axes:
- **Importance** (X-axis): Net Profit LC percentile vs market peers
- **Productivity** (Y-axis): Customer Penetration (40) + Order Frequency (30) + Average Basket Value (30)

---

## Section 2: Segment Definitions
- **Key Accounts**: Importance > 15 AND Productivity >= 40
- **Standard**: Importance > 15 AND Productivity < 40
- **Niche**: Importance <= 15 AND Productivity >= 40
- **Long Tail**: Importance <= 15 AND Productivity < 40

---

## Section 3: Quarterly Monitoring
Check segment distribution monthly to detect drift (tolerance: ±5%):
- Key Accounts: target ~13%
- Standard: target ~15%
- Niche: target ~6%
- Long Tail: target ~66%

---

## Section 4: Weight Corrections & Updates

### Weight Correction: 40-30-30 → 30-30-40 (April 25, 2026)

**Change:** 
- ABV weight reduced from 40 to 30 points
- Customer Penetration weight increased from 30 to 40 points
- Frequency unchanged at 30 points

**Rationale:** 
- ABV has negative correlation (r=-0.0790) with business outcomes
- Penetration (r=0.2718) is the strongest predictor of commercial value
- Primary weight must align with mathematical evidence, not arbitrary convention

**Impact on Suppliers:**
- Suppliers with high ABV + low penetration (<1%) correctly reclassified
  - Example: Supplier 357 (penetration=0.79%, ABV=95.5) moves from Key Accounts → Standard
  - Example: Supplier 54 (penetration=0.60%, ABV=84.9) moves from Key Accounts → Standard
  
- Suppliers with high penetration correctly promoted:
  - Supplier 11 (Pepsico) remains Key Accounts
  - Supplier 51 (Backus) remains Key Accounts

**Validation:** Tested across all 21 countries on 2026-03-01 data. New scheme optimal on 8/21 countries by KA profit.

---

## Section 5: Historical Context

Prior weight schemes tested (April 2026):
- 40-30-30: r=0.2433 correlation (2.1% higher KA profit than 30-30-40)
- 50-30-20: r=0.1788 correlation (abandoned due to amplifying negative predictor)
- 30-30-40: r=0.2718 correlation (mathematically optimal)
- 30-35-35: r=0.2597 correlation (regional favorite, but less consistent globally)
- 45-25-30: r=0.2501 correlation (compromise, not selected)

Decision: Adopt 30-30-40 for mathematical rigor and commercial alignment.

