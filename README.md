# Realistic Hadoop Data Processing Examples

This code is to accompany [my blog post on map reduce frameworks][1]

The point of the code in this repository is to provide an implementation for a business question (listed below) in each of the major Map Reduce frameworks.

Each implementation will get it's own subdirectory with it's own build and running instructions. Each framework will also get an accompanying test, and an in-depth walkthrough about implementation details.

The following implementations are complete:

* Java map reduce - [walkthrough](http://blog.matthewrathbone.com/2013/02/09/real-world-hadoop-implementing-a-left-outer-join-in-hadoop-map-reduce.html)
* Scoobi - [walkthrough](http://blog.matthewrathbone.com/2013/11/03/real-world-hadoop---implementing-a-left-outer-join-with-scoobi.html)
* Scalding - [walkthrough](http://blog.matthewrathbone.com/2015/10/20/2015-10-20-scalding-tutorial.html)
* Cascading - [walkthrough](http://blog.matthewrathbone.com/2015/06/25/real-world-hadoop---implementing-a-left-outer-join-in-java-with-cascading.html)
* Hive  - [walkthrough](http://blog.matthewrathbone.com/2013/02/20/real-world-hadoop---implementing-a-left-outer-join-in-hive.html)
* Pig - [walkthrough](http://blog.matthewrathbone.com/2013/04/07/real-world-hadoop---implementing-a-left-outer-join-in-pig.html)


## The problem


### The Data

We have two datasets: customers, and transactions.

Customer Fields:
* id (1)
* email (matthew@example.com)
* language (EN)
* location (US)

Transaction Fields:
* transaction-id (1)
* product-id (1)
* user-id (1)
* purchase-amount (19.99)
* product-description (a rubber chicken)

These two datasets are stored in tab-delimited files somewhere on HDFS.

### The Question

For each product, we want to know the number of locations in which that product was purchased.

That's it!

In the real world, we might have other questions, like the number of purchases per location for each product.



[1]: http://blog.matthewrathbone.com/post/39783477991/a-quick-guide-to-hadoop-map-reduce-frameworks
[2]: https://github.com/rathboma/hadoop-framework-examples/tree/master/java-mapreduce
