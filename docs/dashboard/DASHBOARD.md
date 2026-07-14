# 📊 E-Commerce Power BI Dashboard

This dashboard was built on top of the **E-Commerce Data Warehouse** (SSIS + SQL Server) and provides four analytical pages covering the full business picture — from executive-level KPIs to granular product and customer insights.

---

## 🗂️ Dashboard Pages

| Page | Purpose |
|------|---------|
| [Executive Overview](#1-executive-overview) | High-level KPIs and revenue trends |
| [Sales Deep Dive](#2-sales-deep-dive) | Category, supplier, campaign & product breakdown |
| [Customer Analysis](#3-customer-analysis) | Customer retention, LTV, and geographic distribution |
| [Returns Profitability](#4-returns-profitability) | Return patterns, refund impact, and at-risk products |

---

## 1. Executive Overview

![Executive Overview](Executive_Page.png)

### Key KPIs
| Metric | Value |
|--------|-------|
| **Total Revenue** | $57.34M |
| **Net Revenue After Returns** | $55.87M |
| **Average Order Value** | $1.07K |
| **Total Orders** | 54K |
| **Total Quantity Sold** | 126K |
| **Return Rate %** | 5.3% |

### Visuals & Insights

- **Total Revenue & Revenue YTD by Month (Line Chart)**  
  Revenue shows a steady plateau from January through September (~$4M–$5M/month), then spikes sharply in **October–December**, indicative of a strong seasonal/holiday effect. Year-to-date (YTD) revenue mirrors this trend, reaching its peak at year end.

- **Total Revenue by Category Name (Pie Chart)**  
  **Electronics** dominates the category mix by a large margin, followed by **Home** and **Clothing**. Categories like Grocery, Books, and Toys represent very small slices — opportunities for either growth or de-prioritization.

- **Product Performance (Bubble Chart)**  
  Products are plotted by quantity sold vs. revenue. A dense cluster sits in the $0–$2M / 0–2K range, with a few outliers at $4M+ revenue — likely high-ticket Electronics items — revealing clear pricing tiers across the catalog.

---

## 2. Sales Deep Dive

![Sales Deep Dive](Sales_Page.png)

### Key KPIs
| Metric | Value |
|--------|-------|
| **Total Revenue** | $57.34M |
| **Total Orders** | 54K |
| **Gross Margin %** | 30.7% |
| **Campaign Revenue** | $40.09M |

### Visuals & Insights

- **Category Revenue (Bar Chart)**  
  **Electronics** leads at ~$20M+, nearly double the second-place **Home** category. Sports, Beauty, Toys, Books, and Grocery contribute minimally. This concentration signals high reliance on a single category.

- **Supplier Revenue (Bar Chart)**  
  **Trusted Suppliers** and **Modern Co.** are the top two suppliers by revenue contribution. The distribution is relatively tiered with no single supplier dominating disproportionately — a healthy supply chain sign.

- **Payment Method Usage (Donut Chart)**  
  Payment methods are fairly distributed across **Debit Card, Apple Pay, Bank Transfer, Google Pay, Credit Card**, and **PayPal** — indicating the platform supports diverse payment preferences without bias toward any single method.

- **Campaign Revenue (Bar Chart)**  
  **New Year Kickoff 2023** is the single highest-performing campaign (~$4M). **Loyalty Rewards 2025** and **Spring Collection 2024** follow closely. This highlights how seasonal and loyalty-based campaigns drive the most revenue.

- **Top Products Table**  
  | Product | Revenue | Qty Sold |
  |---------|---------|---------|
  | Nikon Z14 | $4.89M | 2,484 |
  | Tefal Frying Pan 15 | $4.01M | 7,603 |
  | Sony Alpha 9 | $4.01M | 1,271 |
  | Dell ThinkPad 10 | $2.95M | 1,408 |
  | Giant Domane 8 | $2.53M | 1,067 |

  The **Tefal Frying Pan 15** (Home category) has the highest quantity sold at 7,603 units — a high-volume, lower-margin product compared to the Electronics flagships.

---

## 3. Customer Analysis

![Customer Analysis](Customer_Page.png)

### Key KPIs
| Metric | Value |
|--------|-------|
| **Total Customers** | 800 |
| **Customer Lifetime Value (CLV)** | $71.67K |
| **Repeat Purchase Rate %** | **65.0%** |
| **Returning Customers** | 520 |
| **New Customers** | 280 |

### Visuals & Insights

- **Repeat Purchase Rate of 65%**  
  Nearly two-thirds of the customer base are repeat buyers — a strong retention signal. Combined with 520 returning vs. 280 new customers, the business has a loyal core audience.

- **New vs. Returning Customers by Month (Grouped Bar)**  
  Returning customers consistently outnumber new ones every month (~500 vs. ~50 per month), showing solid retention but a potential bottleneck in customer acquisition.

- **Customer Lifetime Value & Revenue Per Returning Customer by Month (Line Chart)**  
  Both CLV and revenue per returning customer spike sharply in **November–December**, aligning with the holiday revenue surge seen in the Executive Overview. The holiday season is largely powered by loyal, returning customers.

- **Top Customers by Orders & CLV**  
  | Name | Orders | CLV |
  |------|--------|-----|
  | Carol Colon | 276 | $173,595.23 |
  | Ashley Sweeney | 234 | $157,209.45 |
  | Jennifer Payne | 244 | $157,105.77 |
  | Stanley Rogers | 240 | $153,281.51 |

  The top customer (Carol Colon) placed 276 orders — exceptionally high frequency, possibly indicating a B2B/reseller or a power buyer segment worth nurturing.

- **Customers per Country (Bar Chart)**  
  The customer base spans **US, AU (Australia), JP (Japan), DE (Germany), CA (Canada), FR (France), and GB (UK)**. The US leads, but international markets represent a meaningful portion — an indicator of cross-border scalability.

---

## 4. Returns Profitability

![Returns Profitability](Returns_Page.png)

### Key KPIs
| Metric | Value |
|--------|-------|
| **Total Refunds** | $1.47M |
| **Gross Margin %** | 30.7% |
| **Avg Days to Return** | 22.4 |
| **Avg Refund Amount** | $490.28 |
| **Return Rate %** | 5.3% |
| **Net Revenue After Returns** | $55.87M |

### Visuals & Insights

- **Return Rate of 5.3%** is moderate for e-commerce. The $1.47M in refunds against $57.34M total revenue erodes ~2.6% of gross revenue — manageable but worth reducing.

- **Return Reason Count by Reason (Bar Chart)**  
  | Reason | Frequency |
  |--------|-----------|
  | Wrong Size | ~760 (highest) |
  | Damaged | ~640 |
  | Not as Described | ~620 |
  | Defective | ~510 |
  | Changed Mind | ~500 |
  | Late Delivery | ~180 |

  **Wrong Size** is the #1 return driver — a direct signal to improve sizing guides or implement AI-powered size recommendations. **Damaged** and **Not as Described** point to packaging and listing quality issues.

- **Total Refunds by Category (Bar Chart)**  
  **Electronics** generates by far the most refund value (~$0.8M), correlating with it being the highest revenue and highest ticket-price category. **Home** follows at ~$0.25M.

- **Revenue vs. Refunds by Month (Line Chart)**  
  Refunds remain nearly flat (close to $0M) throughout the year even as revenue spikes in Q4. The holiday surge does **not** proportionally increase returns — a strong positive signal that fulfillment quality stays consistent during peak demand.

- **Products with Highest Return Counts**  
  | Product | Return Rate | Total Refunds | Returns Count |
  |---------|------------|---------------|---------------|
  | JBL WH-1000XM11 | 2.8% | $10,001.89 | 20 |
  | Hasbro Party 12 | 2.8% | $2,013.33 | 20 |
  | Giant Domane 8 | 2.3% | $53,755.63 | 20 |
  | Apple ThinkPad 10 | 2.4% | $44,649.72 | 18 |

  **Giant Domane 8** and **Apple ThinkPad 10** carry high refund dollar values despite moderate return counts — high-ticket items that warrant closer quality and listing monitoring.

---

## 🔑 Cross-Page Key Takeaways

1. 🏆 **Electronics is the engine** — drives the most revenue ($20M+), but also the most refunds ($0.8M). Quality control and product descriptions here are critical.
2. 📅 **Seasonality is real** — a sharp Q4 spike (Oct–Dec) across revenue, CLV, and customer activity. Campaigns and inventory should be front-loaded before this window.
3. 🔁 **Retention is strong (65% repeat rate)** — the primary growth lever is new customer acquisition, not retention, which is already healthy.
4. 📦 **Wrong Size & Damaged are the top return drivers** — process improvements in sizing documentation and packaging can directly reduce the $1.47M refund bill.
5. 📣 **Campaigns are highly effective** — $40.09M of $57.34M (70%) revenue is campaign-attributed. New Year Kickoff and Loyalty Rewards campaigns are clear top performers to double down on.
