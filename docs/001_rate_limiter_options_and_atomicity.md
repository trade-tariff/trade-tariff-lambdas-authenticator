# 1. Rate Limiter Atomicity and Concurrency Strategy

- **Status:** Proposed
- **Date:** 2025-11-23

## Context and Problem Statement

The current V2 hybrid rate limiter (`rateLimiterHybridMemoryDynamoV2.js`) is designed for extremely low latency. It achieves this by using an optimistic, "fire-and-forget" model: it allows or denies requests based on a local in-memory cache and asynchronously updates a central token bucket in DynamoDB.

Testing has revealed a side effect of this optimistic approach: under high-concurrency, distributed loads (where many requests arrive at different Lambda instances simultaneously), the system is prone to "over-issuance" or "token undercounting". Multiple instances can read the same token count from the central store _before_ any of them can persist a decremented value. This results in more requests being allowed than the configured limit, effectively rewarding clients for sending highly concurrent traffic.

This non-atomic behavior is unsettling and undermines the predictability of the rate limit. We need to decide on a strategy to address this, balancing the need for accuracy, low latency, and operational complexity.

## Decision Drivers

- **Accuracy:** The need for strict, predictable enforcement of rate limits.
- **Latency:** The desire to keep API response times as low as possible, a primary goal of using Lambda@Edge.
- **Implementation Effort:** The amount of work required to implement and maintain the solution.
- **Operational Complexity:** The cost and effort of managing any new infrastructure.

## Considered Options

### Option 1: Tune the Optimistic Model

A pragmatic approach that accepts the optimistic model but mitigates its effects.

- **Solution:** Intentionally set the configured rate limit to a lower value (e.g., 400 RPM instead of 500 RPM). This treats the inevitable over-issuance as a predictable buffer, pushing the _effective_ rate limit closer to the desired target.
- **Work Involved:** **Minimal.** Requires only changing configuration values.

### Option 2: Adopt the Fully Atomic Model

This approach prioritizes accuracy over latency by using a pessimistic locking strategy.

- **Solution:** Switch to using the existing `rateLimiterAtomicDynamoDb.js` implementation. This function `await`s a conditional `UpdateItem` call to DynamoDB _before_ responding to the user, guaranteeing that a token is successfully claimed before the request is allowed.
- **Work Involved:** **Low.** The code already exists. The work involves changing which rate limiter is invoked.
- **Downside:** This will increase latency for every single request, as each one must wait for a database round-trip.

### Option 3: Architect a "Token Broker" Service

The most robust, scalable solution that provides both accuracy and high performance.

- **Solution:**
  1. Deploy a centralized, high-speed, in-memory data store like **Redis (ElastiCache)**.
  2. Create a simple, regional API (e.g., via API Gateway and a standard Lambda) that acts as a "Token Broker" in front of Redis.
  3. Modify the Lambda@Edge function to make a synchronous call to this new service instead of DynamoDB. The broker would use Redis's atomic commands (`DECR`) to dispense tokens.
- **Work Involved:** **High.** This requires significant effort to design, build, deploy, and monitor a new piece of critical infrastructure.

## Decision Outcome

**Proposed:** Start with **Option 1 (Tune the Optimistic Model)**.

This is the most pragmatic first step. It immediately addresses the issue of over-issuance in a predictable way with no additional code, cost, or latency.

If monitoring reveals that the tuned limit is still not sufficient and stricter guarantees are required, **Option 2 (Adopt the Fully Atomic Model)** should be considered next, accepting the trade-off of increased latency.

**Option 3 (Token Broker)** is the best long-term architectural pattern for high-throughput, accurate rate limiting, but it should only be pursued if the other options prove insufficient for the business requirements, given its higher implementation and operational cost.
