# Example

## 1. Imbalance partition assignment

Create a topic with 3 partitions, run 2 consumer. Producer should have TPS >
sum TPS of consumer.  Using the default round robin partitioner,  2 out of 3
partitions should have some increasing lag. The Juggler Balancer will take care
so that consumer Lag won't get out of control.


## 2. Uneven work load

Number of partitions should be multiple of consumer. Now some consumer have
lower TPS than others, and their partitions should have increasing lag
accordingly. The Juggler Balancer should kicks in now to make sure that consumer
Lag won't get out of control.
