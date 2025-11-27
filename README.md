Spring for Apache Kafka
[![Build Status](https://github.com/spring-projects/spring-kafka/actions/workflows/ci-snapshot.yml/badge.svg)](https://github.com/spring-projects/spring-kafka/actions/workflows/ci-snapshot.yml)
[![Revved up by Develocity](https://img.shields.io/badge/Revved%20up%20by-Develocity-06A0CE?logo=Gradle&labelColor=02303A)](https://ge.spring.io/scans?search.rootProjectNames=spring-kafka)
==================

# Code of Conduct

Please see our [Code of conduct](https://github.com/spring-projects/.github/blob/main/CODE_OF_CONDUCT.md).

# Reporting Security Vulnerabilities

Please see our [Security policy](https://github.com/spring-projects/spring-kafka/security/policy).

# Checking out and Building

To check out the project and build from source, do the following:

    git clone git://github.com/spring-projects/spring-kafka.git
    cd spring-kafka
    ./gradlew build

Java 17 or later version is recommended to build the project.

If you encounter out of memory errors during the build, change the `org.gradle.jvmargs` property in `gradle.properties`.

To build and install jars into your local Maven cache:

    ./gradlew install

To build API Javadoc (results will be in `build/api`):

    ./gradlew api

To build reference documentation (results will be in `spring-kafka-docs/build/site`):

    ./gradlew antora

To build complete distribution including `-dist`, `-docs`, and `-schema` zip files (results will be in `build/distributions`)

    ./gradlew dist

# Using Eclipse

To generate Eclipse metadata (.classpath and .project files), do the following:

    ./gradlew eclipse

Once complete, you may then import the projects into Eclipse as usual:

 *File -> Import -> Existing projects into workspace*

Browse to the *'spring-kafka'* root directory. All projects should import free of errors.

# Using IntelliJ IDEA

To generate IDEA metadata (.iml and .ipr files), do the following:

    ./gradlew idea

# Resources

For more information, please visit the Spring Kafka website at:
[Reference Manual](https://docs.spring.io/spring-kafka/reference/index.html)

# Contributing to Spring Kafka

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Spring Community Forums.  
Please help out on the [StackOverflow](https://stackoverflow.com/questions/tagged/spring-kafka) by responding to questions and joining the debate.
* Create [GitHub issues](https://github.com/spring-projects/spring-kafka/issues) for bugs and new features and comment and vote on the ones that you are interested in.
* GitHub is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](https://help.github.com/forking/).  
If you want to contribute code this way, please reference a GitHub issue as well covering the specific issue you are addressing.

All commits must include a _Signed-off-by_ trailer at the end of each commit message to indicate that the contributor agrees to the Developer Certificate of Origin.
For additional details, please refer to the blog post [Hello DCO, Goodbye CLA: Simplifying Contributions to Spring](https://spring.io/blog/2025/01/06/hello-dco-goodbye-cla-simplifying-contributions-to-spring).
The DCO does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do.
Active contributors might be asked to join the core team and given the ability to merge pull requests.

## Code Conventions and Housekeeping
None of these is essential for a pull request, but they will all help.
They can also be added after the original pull request but before a merge.

* Use the Spring Framework code format conventions (import `eclipse-code-formatter.xml` from the root of the project if you are using Eclipse).
* Make sure all new `.java` files to have a simple Javadoc class comment with at least an `@author` tag identifying you, and preferably at least a paragraph on what the class is for.
* Add the ASF license header comment to all new `.java` files (copy from existing files in the project)
* Add yourself as an `@author` to the `.java` files that you modify substantially (more than cosmetic changes).
* Add some Javadocs and, if you change the namespace, some XSD doc elements.
* A few unit tests would help a lot as well - someone has to do it.
* If no-one else is using your branch, please rebase it against the current main (or another target branch in the main project).

# Getting Support
Use the [`spring-kafka` tag on Stack Overflow](https://stackoverflow.com/questions/tagged/spring-kafka) to ask questions; include code and configuration and clearly explain your problem, providing an [MCRE](https://stackoverflow.com/help/minimal-reproducible-example) if possible.
[Commercial support](https://spring.io/support) is also available.

# License

Spring Kafka is released under the terms of the Apache Software License Version 2.0 (see [LICENSE.txt](LICENCE.txt)).
