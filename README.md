# Flink Async HTTP Processor

This repository contains a sample project that demonstrates how to use Flink Async I/O to call HTTP external services asynchronously. The project is built using Apache Flink 1.17 and Java 11.

Setup
To build and run the project, you will need to have the following installed on your system:

Apache Flink 1.17
Java 11
Maven 3.x
Once you have these dependencies installed, you can build the project using Maven:

```
mvn clean package
```
Usage
To use the async HTTP client in your Flink application, you can create an instance of the AsyncHttpClient class and call its sendAsync() method to make asynchronous HTTP requests. Here's an example:

```
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;

public class HttpAsyncFunction implements AsyncFunction<String, String> {

    private transient HttpClient httpClient;

...

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) {
        httpClient.sendAsync(HttpRequest.newBuilder()
                .uri(URI.create("https://backend.com/api"))
                .GET()
                .build())
            .thenApply(HttpResponse::body)
            .thenAccept(resultFuture::complete);
    }

...

}
```

Contributing
If you'd like to contribute to this project, please create a pull request with your changes. Before submitting your pull request, please make sure that your code follows the project's coding standards and that all tests pass.
