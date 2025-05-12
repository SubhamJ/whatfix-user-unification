Basic Flink Based Real-Time Job for the mentioned problem statement

Question 1: User Identity Unification for Analytics
Context:
Product collects events from multiple platforms (web, mobile, support chat, ai agents, desktop), and
users can be anonymous or logged-in. You need to unify user identities to create cohort-based
targeting and track their full lifecycle journey.
Problem Statement:
Design a system that ingests events and resolves user identity across anonymous and logged-in
sessions. The system should support:
● Real-time enrichment and updating of unified profiles
● Historical stitching of identities when a user logs in
Scope/Constraints:
● 10M MAUs
● Daily volume ~150M events
● Queryable profile store for targeting & cohort building
● Backfill logic to stitch past anonymous sessions
What to Expect from the Candidate:
● Identity resolution model (user_id, device_id, cookie_id, extension_id)
● Data modeling of unified profile
● Real-time enrichment vs batch unification
● Data quality: merging conflicts, de-duplication of identities, missing identifiers
● Storage choice
FAQs for Question 1: User Identity Unification for Analytics
Q: Should I assume the identity data is directly sent with each event?
A: Yes. Assume events come with available identifiers such as cookie_id, device_id, and optionally
user_id. User_id can be an email-id or any identifier post logged-in.
Q: How do I handle identity transitions (e.g., anonymous to logged-in)?
A: Design for real-time enrichment (for new events). This is a key expectation in the problem.
Q: Is the stitching expected to happen across sessions or devices?
A: Yes. Identity stitching should consider merging across:
● Multiple sessions from the same device
● Multiple devices used by the same user (if identifiable through login or email)
Q: Do I need to create the cohort system as well?
A: No. Just design a profile store that can serve as the source of truth for downstream cohort building.
You are not required to implement the cohort logic itself.
Q: Should I propose a real-time system only?
A: Yes. You're expected to cover real-time:
● Real-time: for updating a profile as soon as a new event arrives
Q: Should I cover deduplication or conflict resolution strategies?
A: Yes. Please consider:
● How to handle multiple conflicting identifiers
● How to prevent duplicate profiles (e.g., one for cookie_id, one for user_id)
Q: What kind of queries should the system support?
A: Think in terms of queries like:
● "All users who have logged in at least once and also used mobile"
● "Users who had 3+ sessions anonymously and later logged in"
Your design should enable fast retrieval of unified profiles that support such filters.
Q: Can I assume use of managed services like BigQuery or Delta Lake?
A: Yes. You're free to choose any cloud-native or open-source technologies, as long as you justify why
they meet the real-time, scalability, and query efficiency requirements.
Q: What level of detail is expected in the data model?
A: We expect:
● Logical schema for unified profile (e.g., user_id, identifiers, device list, session count)
● Outline of how data is organized for fast lookup (e.g., key-value, columnar store)
