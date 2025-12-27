import pandas as pd
import random
from datetime import datetime, timedelta

NUM_ROWS_TOTAL = 1000
START_ID = 101
current_time = datetime(2025, 11, 6, 9, 0, 0)

PEOPLE = ["Alex (CEO)", "Sarah (CTO)", "James (Lead Dev)", "Maya (Dev)", "Leo (Dev)", "Chloe (Product)", "Sam (Growth)"]
EXTERNAL_POOLS = ["VC Partner - Sequoia", "AWS Support", "Stripe Billing", "Enterprise Lead - Walmart", "Vanta Compliance", "Security Researcher", "Datadog"]
VC_FIRMS = ["Sequoia", "Andreessen Horowitz", "Benchmark", "Index Ventures", "First Round Capital"]
ENTERPRISE_PROSPECTS = ["Goldman Sachs", "Delta Airlines", "Shopify", "Walmart", "FedEx"]
FEATURES = ["Real-time Collaboration", "SOC2 Audit Logging", "Dark Mode UI", "API v2", "Bulk Data Export", "AI Suggestion Engine", "Real-time Feed", "Causal Engine", "Sandbox Mode"]

EMAIL_TEMPLATES = [
    "Subject: Q4 Technical Roadmap and Infrastructure Scalability. Hi Sarah, following up on our sync regarding the move to a multi-region architecture. As we approach the {milestone} mark in user growth, the single-region RDS setup is becoming a single point of failure. I've attached a proposal to migrate the primary cluster to a 'Global Database' configuration. This will increase our monthly AWS spend by approx ${cost}, but it reduces P99 latency for our EU customers by 60%. We need this finalized before the Series A close so we can include the CapEx in the projections.",
    "Subject: Urgent: Vulnerability Disclosure - Potential SQL Injection. Dear Security Team, I am an independent security researcher. While performing a gray-box assessment of your /{endpoint} endpoint, I discovered that the {parameter} parameter does not properly sanitize input, allowing for a time-based blind SQL injection. I have successfully extracted the database version as a proof-of-concept. I have not accessed any user data. Please acknowledge this report within 24 hours to begin the coordinated disclosure process.",
    "Subject: Monthly Investor Update: Growth, Burn, and Unit Economics. Hi Team, October was a record-breaking month. Our Net Revenue Retention (NRR) hit {nrr}%, driven by the successful upsell of the 'Advanced Security' module. Gross margins improved to {margin}% as James optimized the data ingestion pipeline. Current Runway: {runway} months. Our main bottleneck remains hiring; we have 4 open roles for Senior Backend Engineers that have been open for over 60 days. We are seeing a slight increase in CAC for the Fintech vertical, which we are addressing via a new content marketing strategy.",
    "Subject: Customer Feedback: Enterprise User Experience. Hi Chloe, I'm the Head of Digital Transformation at {company}. We've been using ScaleFlow for 3 months now. While the core engine is the fastest we've seen, the 'Reporting' dashboard is missing a critical 'PDF Export' feature that our board requires for monthly audits. If we can't automate this, our team has to spend 4 hours a month manually copying data. Is this on your short-term roadmap? We'd love to expand our seat count from 50 to 200 if this is resolved.",
]

CHAT_TEMPLATES = [
    "PR #{id} is failing the integration tests on the {service} module. It looks like a race condition in the {tech} driver. James, can you take a look?",
    "I'm seeing a spike in memory usage on the {region} nodes. It's climbing at a rate of 50MB/hour. Definitely a leak. I'm taking a heap snapshot now.",
    "The demo with {company} went incredibly well. They loved the {feature} but asked if we support {integration} natively. We might need to build a shim for that.",
    "Just merged the fix for the {bug} bug. It was a stupid off-by-one error in the pagination logic. Users should see the correct counts now.",
    "Can we talk about the technical debt in the {module}? Every time we touch it, something else breaks. I want to propose a 'Cleanup Friday' for the next month.",
    "We just hit {count} concurrent WebSockets! The cluster handled the transition perfectly. Kudos to the infra team for the load balancer tuning.",
    "I just saw the budget alert for {service}. We're spending way too much on uncompressed logs. I'm going to update the logging config to use Zstd compression.",
]

def get_random_metrics():
    mrr = random.randint(150, 450)
    churn = round(random.uniform(0.8, 3.5), 1)
    dau = random.randint(20, 80)
    return f"${mrr}k MRR, {churn}% Churn, and {dau}k DAU"

def generate_dynamic_email():
    category = random.choices(["investor", "enterprise", "analytics", "security", "legal"], weights=[30, 30, 20, 10, 10])[0]
    
    if category == "investor":
        firm = random.choice(VC_FIRMS)
        subject = random.choice([f"Follow up: {firm} / Startup X Intro", "Quick question on your Q4 growth", "Intro from our portfolio partner"])
        body = f"I've been tracking your progress on Product Hunt. The recent jump to {get_random_metrics()} is impressive for a Series A stage. We'd love to dig into your unit economics and 'Time to Value' metrics. Are you around for a brief sync on Tuesday?"
        sender = f"Partner at {firm}"
        
    elif category == "enterprise":
        client = random.choice(ENTERPRISE_PROSPECTS)
        subject = random.choice([f"Procurement Request: {client}", "Security Questionnaire", "SLA Requirements for Enterprise Tier"])
        body = f"Our team at {client} is ready to move forward with the {random.choice(FEATURES)} module. However, our legal team requires a signed SLA guaranteeing 99.99% uptime and a review of your Disaster Recovery plan. Can you provide the logs for your last Multi-AZ failover test?"
        sender = f"Procurement Lead ({client})"
        
    elif category == "analytics":
        subject = f"Weekly System & Growth Report: {datetime.now().strftime('%Y-%W')}"
        body = f"[AUTO-GENERATED] Performance Summary: \n- Infrastructure Spend: ${random.randint(5000, 15000)} (+{random.randint(2, 10)}%)\n- Error Rate: {random.uniform(0.01, 0.05):.3}%\n- Top Feature Usage: {random.choice(FEATURES)}\n- Critical Alerts: {random.randint(0, 3)} unresolved Sentry issues."
        sender = "Internal Analytics Bot"
        
    elif category == "security":
        subject = "Urgent: Vulnerability Disclosure"
        body = f"A security researcher has identified a potential SQL injection vulnerability in the {random.choice(FEATURES)} endpoint. Although it's shielded by the WAF, we need a code-level fix within 24 hours to maintain our SOC2 readiness status."
        sender = "Security Auditor"
        
    else: # Legal/HR
        subject = "Board Consent / Employee Option Grants"
        body = f"Attached is the board consent for the latest hiring round. We need to finalize the equity grants for the new Senior SRE and the Lead Designer. Please sign via DocuSign so we can stay on schedule for the {random.choice(VC_FIRMS)} due diligence."
        sender = "External Counsel"
        
    return f"Subject: {subject}. {body}", sender

def generate_dynamic_chat():
    topic = random.choices(["scaling", "bugs", "shipping", "metrics"], weights=[30, 25, 30, 15])[0]
    
    if topic == "scaling":
        return f"Database is hitting {random.randint(70, 95)}% CPU. James, can we optimize the query for {random.choice(FEATURES)}? We're seeing some lag in production."
    elif topic == "bugs":
        return f"Sentry alert: {random.choice(['NullPointerException', 'TimeoutError', 'AuthFailure'])} on the {random.choice(FEATURES)} module. Maya, taking a look?"
    elif topic == "shipping":
        return f"PR #{random.randint(100, 900)} for {random.choice(FEATURES)} is merged! Deploying to staging now. 🚀"
    else: # metrics
        return f"Check this out: {get_random_metrics()}. The marketing push for {random.choice(FEATURES)} is clearly working!"

def generate_message(is_chat):
    use_dynamic = random.random() > 0.5
    
    if is_chat:
        chat_sender = random.choice(PEOPLE)
        email_sender = ""
        if use_dynamic:
            content = generate_dynamic_chat()
        else:
            content = random.choice(CHAT_TEMPLATES).format(
                id=random.randint(1000, 9999),
                service=random.choice(["Auth", "Ingestion", "Billing", "Search", "Analytics"]),
                tech=random.choice(["Redis", "Postgres", "Kafka", "gRPC"]),
                region=random.choice(["us-east-1", "eu-west-1", "ap-southeast-1"]),
                company=random.choice(["JPMC", "Uber", "Airbnb", "FedEx"]),
                feature=random.choice(FEATURES),
                integration=random.choice(["Snowflake", "Databricks", "Salesforce"]),
                bug=random.choice(["caching", "auth-token", "UI flicker", "latency"]),
                module=random.choice(["legacy-api", "frontend-state", "worker-queue"]),
                count=random.randint(5000, 25000)
            )
    else:
        chat_sender = ""
        if use_dynamic:
            content, email_sender = generate_dynamic_email()
        else:
            email_sender = random.choice(EXTERNAL_POOLS)
            content = random.choice(EMAIL_TEMPLATES).format(
                milestone=random.choice(["100k user", "5M event-per-second", "Series A"]),
                cost=random.randint(1500, 5000),
                endpoint=random.choice(["v1/users", "v2/transactions", "admin/config"]),
                parameter=random.choice(["user_id", "org_id", "filter_query"]),
                nrr=random.randint(110, 145),
                margin=random.randint(70, 85),
                runway=random.randint(8, 22),
                company=random.choice(["Global Logistics", "FinanceStream", "HealthTech Inc"])
            )
            
    return chat_sender, email_sender, content

def generate_data():
    global current_time
    data = []
    
    for i in range(START_ID, START_ID + NUM_ROWS_TOTAL):
        is_chat = random.random() > 0.15 # 85% chat, 15% email
        chat_sender, email_sender, content = generate_message(is_chat)
        
        data.append([i, str(is_chat).lower(), chat_sender, email_sender, current_time.strftime('%Y-%m-%d %H:%M:%S'), content])
        
        if is_chat:
            current_time += timedelta(minutes=random.randint(2, 15))
        else:
            current_time += timedelta(hours=random.randint(1, 4))
            
    return data

if __name__ == "__main__":
    new_data = generate_data()
    df_new = pd.DataFrame(new_data, columns=["id", "is_chat", "chat_sender", "email_sender", "timestamp", "content"])
    
    try:
        prev = pd.read_csv("sample.csv")
        full_df = pd.concat([prev, df_new], ignore_index=True)
        full_df.to_csv("full_data.csv", index=False)
        print(f"Successfully generated {NUM_ROWS_TOTAL} rows and saved to full_data.csv")
    except FileNotFoundError:
        df_new.to_csv("full_data.csv", index=False)
        print(f"sample.csv not found. Saved {NUM_ROWS_TOTAL} rows to full_data.csv")
