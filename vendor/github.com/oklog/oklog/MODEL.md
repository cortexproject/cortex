# Model

This is a very preliminary braindump on some ideas for a system model.
I am no academic, and it's been a long time since I've taken my queueing theory class.
This is all unvalidated, and may be wildly off-base.
I welcome corrections with an open heart and mind.

## Queueing theory

First, [an introduction to some queueing theory](http://www.perfdynamics.com/Tools/PDQ.html).

```
 <--------R--------->
 <---W--->  <---S--->
           +---------+
   λ       |         |  X
 --> • • • |    •    |--> •
           |         |
           +---------+
 <---Q--->
```


- The average arrival rate **λ** into the queue
- The average service time **S** in the server

From λ and S we can deduce other properties of the system.

- The throughput coming out of the server: **X** = λ
- The average residence time a customer spends getting through the queue: **R** = S / (1 - λS)
- The utilization of the server: **ρ** = λS
- The average queue length: **Q** = λR
- The average waiting time before service: **W** = R - ρ

For testing we can reasonably fix λ to some constant value.
In contrast, S will be a function of the size of the work unit (record).
For now let's fix each record at N bytes. Then, S = f(N).

## Our components

- Producer — Forward — Ingest — Store

The producer establishes the arrival rate of records λr, and the size of each record Nr.

- λr
- Sr = f(Nr)

We'll need to run experiments to figure out the service time function Sr.
For now let's assume one nanosecond per byte.

- Sr = Nr * 1ns

Between the producer and the forwarding agent exists a pipe buffer.
This has a maximum bandwidth which affects througput.

- TODO

The first queue exists at the forward agent.
The forwarder just writes records to the connection.
So the service time at the forwarder is simple.

- Sforward = Per-record consumption overhead + Cost per byte to write to the connection
- Sforward = Oconsumption(1) + Wconnection(Nr)

Again, we'll need to experiment to figure out Oconsumption and Wconnection.
For now, let's fix them at some guesses.

- Oconsumption = 10ns
- Wconnection = Nr * 2ns

Between the forwarder and the ingester exists a network.
This has a maximum bandwidth which affects throughput.

- TODO

Then, we reach the ingester.
First, records are taken from connections and written to the active segment.
This follows a similar model of the forwarder.

- Singest = Per-record consumption overhead + Cost per byte to write to the segment
- Singest = Oconsumption(1) + Wsegment(Nr)

Again, we'll need to experiment to figure out Wsegment.
For now, let's fix it with a guess.

- Wsegment = Nr * 1ns

Segments become available for consumption after they reach a defined age A or size S.
A imposes a maximum age, and S imposes a maximum size.
The ingester effectively changes the unit of work, from record to segment.
So, we derive a new rate of segments λs, and segment size Nr.

- Ns = f(λr, Nr, A, S)
- λs = f(λr, Nr, A, S)

Let λr = 100 records per second, Nr = 500 bytes, A = 1 second, and S = 1 megabyte.
100 records per second times 500 bytes per record yields 50000 bytes per second.
This triggers A = 1 second, but doesn't trigger S = 1 megabyte.
So Ns will be 50000 bytes, and λs will be 1 segment per second.

Let λr = 10000 records per second, Nr = 500 bytes, A = 1 second, and S = 1 megabyte.
10000 records per second times 500 bytes per record yields 5000000 bytes per second.
This triggers S = 1 megabyte at (1MB / 5000000B) * second = 200ms.
So Ns will be 1MB, and λs will be 5 segments per second.

Segments are made available by ingesters but consumed by store nodes.
Store nodes consume when they have capacity, choosing arbitrarily from ingest nodes.
We can model this as a virtual queue.
Available segments across all ingesters form the virtual queue of waiting work.
And all store nodes collectively form a single virtual server.

- TODO
