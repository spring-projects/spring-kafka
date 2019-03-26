Spring Kafka [<img src="https://build.spring.io/plugins/servlet/wittified/build-status/SK-MAS">](https://build.spring.io/browse/SK-MAS)
[![Join the chat at https://gitter.im/spring-projects/spring-kafka](https://badges.gitter.im/spring-projects/spring-kafka.svg)](https://gitter.im/spring-projects/spring-kafka?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
==================

# Checking out and Building

To check out the project and build from source, do the following:

    git clone git://github.com/spring-projects/spring-kafka.git
    cd spring-kafka
    ./gradlew build

The Java SE 7 or higher is recommended to build the project.

If you encounter out of memory errors during the build, increase available heap and permgen for Gradle:

    GRADLE_OPTS='-XX:MaxPermSize=1024m -Xmx1024m'

To build and install jars into your local Maven cache:

    ./gradlew install

To build api Javadoc (results will be in `build/api`):

    ./gradlew api

To build reference documentation (results will be in `build/reference`):

    ./gradlew reference

To build complete distribution including `-dist`, `-docs`, and `-schema` zip files (results will be in `build/distributions`)

    ./gradlew dist

# Using Eclipse

To generate Eclipse metadata (.classpath and .project files), do the following:

    ./gradlew eclipse

Once complete, you may then import the projects into Eclipse as usual:

 *File -> Import -> Existing projects into workspace*

Browse to the *'spring-kafka'* root directory. All projects should import
free of errors.

# Using IntelliJ IDEA

To generate IDEA metadata (.iml and .ipr files), do the following:

    ./gradlew idea

# Resources

For more information, please visit the Spring Kafka website at:
[Reference Manual](https://docs.spring.io/spring-kafka/docs/current/reference/html/)

# Contributing to Spring Kafka

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Spring Community Forums.  Please help out on the [StackOverflow](https://stackoverflow.com/questions/tagged/spring-kafka) by responding to questions and joining the debate.
* Create [GitHub issues](https://github.com/spring-projects/spring-kafka/issues) for bugs and new features and comment and vote on the ones that you are interested in.
* Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](https://help.github.com/forking/).  If you want to contribute code this way, please reference a JIRA ticket as well covering the specific issue you are addressing.
* Watch for upcoming articles on Spring by [subscribing](https://www.springsource.org/node/feed) to springframework.org

Before we accept a non-trivial patch or pull request we will need you to sign the [contributor's agreement](https://support.springsource.com/spring_committer_signup).
Signing the contributor's agreement does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do.
 Active contributors might be asked to join the core team, and given the ability to merge pull requests.

## Code Conventions and Housekeeping
None of these is essential for a pull request, but they will all help.
  They can also be added after the original pull request but before a merge.

* Use the Spring Framework code format conventions (import `eclipse-code-formatter.xml` from the root of the project if you are using Eclipse).
* Make sure all new .java files to have a simple Javadoc class comment with at least an @author tag identifying you, and preferably at least a paragraph on what the class is for.
* Add the ASF license header comment to all new .java files (copy from existing files in the project)
* Add yourself as an @author to the .java files that you modify substantially (more than cosmetic changes).
* Add some Javadocs and, if you change the namespace, some XSD doc elements.
* A few unit tests would help a lot as well - someone has to do it.
* If no-one else is using your branch, please rebase it against the current master (or other target branch in the main project).

# License

Spring Kafka is released under the terms of the Apache Software License Version 2.0 (see license.txt).

# Code of Conduct

This project adheres to the Contributor Covenant link:CODE_OF_CONDUCT.adoc[code of conduct].
By participating, you  are expected to uphold this code. Please report unacceptable behavior to
spring-code-of-conduct@pivotal.io.
