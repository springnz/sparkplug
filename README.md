# Sparkplug

**A framework for creating composable and pluggable data processing pipelines using Apache Spark, and running them on a cluster.**

*Please note that this project is early stage work in progress, and will be subject to some breaking changes and aggressive refactoring. It will however be getting a lot of love in the next couple of months.*

Apache Spark is great, but not everything you need to use it successfully in production environments comes out the box.

This project aims to bridge the gap. In particular, it addresses two specific requirements.

1. Creating data processing pipelines that are easy to reuse and test in isolation.
2. Providing a lightweight mechanism for launching and executing Spark processes on a cluster. 

These two requirements are quite different. Indeed it is possible to use Sparkplug for either of them without taking advantage of the other. For example it is possible to create composable data pipelines as described below, then execute them directly, or using any other Spark cluster execution or job manager of your choice.

## Data processing pipelines

The key abstraction here is the `SparkOperation` monad.

```
sealed trait SparkOperation[+A] {
  def run(ctx: SparkContext): A
}
```

`SparkOperation` are typically created using the companion class. Here is the simplest possible example:

```
val textRDDOperation = SparkOperation[RDD[String]] {
  ctx ⇒ ctx.makeRDD("There is nothing either good or bad, but thinking makes it so".split(' '))
}
```

This is a simple `SparkOperation` that takes a string and returns a `RDD[String]` consisting of the words of a sentence.

We can then use this `SparkOperation` to create another operation.

```
val letterCount: SparkOperation[Long] = for {
    logData ← textRDDProvider
  } yield logData.filter(_.contains("a")).count()
}
```

In this case we are counting the number of words that contain the letter 'a'.

Proceeding as in this simple example, we can create complex data processing pipelines, mainly using monadic operations. 

These include:

* Bread and butter map and flatmap to compose operations (as above).
* Combining operations (e.g. convert a tuple of `SparkOperation`s to a `SparkOperation` of tuples).
* Sequence operations (e.g. convert a list of `SparkOperation`s to a `SparkOperation` of list). 

Then once we have composed the `SparkOperation` as desired, it is against a given `SparkContext`.

val answer = letterCount.run(sparkContext)

The types of `SparkOperation`s are typically, at least until the end of the pipeline, `RDD`s.

### Why go to all this trouble?

For simple processes, as above, it is overkill. However, non-trivial data processing pipelines typically involve many stages, and often there are many permutations over which these steps may be applied in different scenarios. 

Splitting the process into discrete, separate operations has two main advantages:

1. `SparkOperation`s, modular in nature, can easily be reused or shared across different data processing pipelines.
2. They can be unit tested in isolation. There are several utilities included in the project that facilitate this. This is covered in the section on testing below.
3. Operations can be glued together using compact functional code.

Note that this pattern involves decoupling the pipeline definition from the pipeline execution, which enables a great deal of flexibility over how one defines pipelines and executes them. 

It does lead to the one drawback in that stack dumps are not normally very meaningful. For this reason good logging and error handling is important.

## Wiring together `SparkOperation` components

TBD

## Execution on a cluster

The recommended mechanism for execution on a cluster (Java or Scala) is as follows:

1. Package your Java / Scala project into a assembly (a single jar with all the transitive dependencies, sometimes called an uber jar).
2. Invoke the `spark-submit` app, passing in the assembly into the command line. Your app is run on the cluster, and `spark-submit` terminates after your app finishes. The Spark project also contains a `SparkLauncher` class, which is a thin Scala wrapper around `spark-submit`.

However, there is still plenty of work to do to coordinate this in a production environment. If you are already doing this kind of work in Scala, the Spark Plug library is very useful.

Another issue is that creating assemblies can lead to all sorts of problems with conflicts in transitive dependencies, which are often difficult to resolve, especially if you don't even know what the these dependencies do. Assembblies can also get large really quickly, and can take a while for `spark-submit` to upload to the cluster.

A third issue is that ideally you want the cluster to be available when a job request arrives. However there is plenty that can be set up in advance in preparation, so that when the job request arrives, there is less that can go wrong. The `spark-submit` command line execution pattern doesn't easily facilitate that.

### How Sparkplug cluster execution works

The use case that Sparkplug cluster execution is particularly well suited to is where your overall technology stack is Scala based, and particular if Akka is a big part of it. If you have a polyglot stack, something like the REST based [Spark Job Server](https://github.com/spark-jobserver/spark-jobserver) may be more suitable.

Sparkplug launcher uses Akka remoting under the hood. Sparkplug launches jobs on the cluster using the following steps:

1. The client has an `ActorSystem` running, and an execution client actor.
2. This client invokes `spark-submit` to run an application on the server.
3. The server starts up it's own `ActorSystem`, and once this is done, sends a message to inform the client.
4. It creates a `SparkContext`, which is then available to service request to run Spark jobs that it may receive. The service is now ready for action. 
5. When a request arrives at the client, it sends a message to the server to process the request.
6. The job is then run by the server and the client is notified when it is done. The final result is streamed back to the client.

The details of how to plug an operation pipeline into the cluster execution... TBD

## Projects

SparkPlug is set up as a sbt multi-project with the following subprojects:

* **sparkplug-core**: The core `SparkOperation` monad and related traits and interfaces.
* **sparkplug-extras**: Components for data access (currently Cassandra and SQL) and utilities for testing.
* **sparkplug-examples**: Several examples for how to create Spark pipelines. A good place to start.
* **sparkplug-executor**: The Server side of the cluster execution component.
* **sparkplug-launcher**: The Client side of the cluster execution component.
