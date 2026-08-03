# Spring for Apache Kafka Contributor Guidelines

Have something you'd like to contribute to **Spring for Apache Kafka**?
We welcome pull requests, but ask that you carefully read this document first to understand how best to submit them;
what kind of changes are likely to be accepted; and what to expect from the Spring team when evaluating your submission.

Please refer back to this document as a checklist before issuing any pull request; this will save time for everyone!

## Code of Conduct

This project adheres to the Contributor Covenant [code of conduct](CODE_OF_CONDUCT.adoc).
By participating, you are expected to uphold this code. Please report unacceptable behavior to
spring-code-of-conduct@spring.io.

## Understand the basics

Not sure what a **pull request** is, or how to submit one? Take a look at GitHub's excellent documentation:
[Using Pull Requests](https://help.github.com/articles/using-pull-requests/) first.

## Search GitHub issues first; create one if necessary

Is there already an issue that addresses your concern?
Search the [GitHub issue tracker](https://github.com/spring-projects/spring-kafka/issues) to see if you can find something similar.
If not, please create a new issue before submitting a pull request unless the change is truly trivial, e.g. typo fixes,
removing compiler warnings, etc.

## Developer Certificate of Origin

All commits must include a _Signed-off-by_ trailer at the end of each commit message to indicate that the contributor agrees to the Developer Certificate of Origin.
For additional details, please refer to the blog post [Hello DCO, Goodbye CLA: Simplifying Contributions to Spring](https://spring.io/blog/2025/01/06/hello-dco-goodbye-cla-simplifying-contributions-to-spring).

## Fork the Repository

1. Go to [https://github.com/spring-projects/spring-kafka](https://github.com/spring-projects/spring-kafka)
2. Hit the "fork" button and choose your own GitHub account as the target
3. For more detail see [Fork A Repo](https://help.github.com/articles/fork-a-repo/).

## Setup your Local Development Environment

1. `git clone --recursive git@github.com:<your-github-username>/spring-kafka.git`
2. `cd spring-kafka`
3. `git remote show`
   _you should see only 'origin' - which is the fork you created for your own GitHub account_
4. `git remote add upstream git@github.com:spring-projects/spring-kafka.git`
5. `git remote show`
   _you should now see 'upstream' in addition to 'origin' where 'upstream' is the Spring repository from which releases are built_
6. `git fetch --all`
7. `git branch -a`
   _you should see branches on origin as well as upstream, including 'main'_

## Building the Project

The project uses the Gradle wrapper (`gradlew` / `gradlew.bat`). Java 17 or later is required.

```bash
# Full build with tests
./gradlew build

# Run tests only
./gradlew test

# Run tests for a specific module
./gradlew :spring-kafka:test

# Run a single test class
./gradlew :spring-kafka:test --tests "org.springframework.kafka.listener.ConcurrentMessageListenerContainerTests"

# Publish to local Maven repository
./gradlew publishToMavenLocal

# Generate API Javadoc (output: build/api)
./gradlew api

# Generate reference documentation (output: spring-kafka-docs/build/site)
./gradlew antora

# Build distribution zips (output: build/distributions)
./gradlew dist
```

## Module Structure

The root project (`spring-kafka-dist`) aggregates all `spring-*` directories automatically via `settings.gradle`.

| Module | Purpose |
|---|---|
| `spring-kafka` | Core library: `KafkaTemplate`, `@KafkaListener`, listener containers, error handlers, transactions |
| `spring-kafka-bom` | Bill of Materials for dependency management |
| `spring-kafka-docs` | Reference documentation (Antora/AsciiDoc) |
| `spring-kafka-test` | Test utilities: `EmbeddedKafkaBroker`, `@EmbeddedKafka`, JUnit 5 support, AssertJ/Hamcrest matchers |

## Architecture

**Layer design**: The library wraps the Apache Kafka client (`org.apache.kafka:kafka-clients`) with Spring-idiomatic abstractions for producing, consuming, and administering Kafka topics.

**Key packages in `spring-kafka`**:
- `annotation` — `@KafkaListener`, `@KafkaHandler`, `@EnableKafka`, `KafkaListenerAnnotationBeanPostProcessor`
- `aot` — GraalVM native image hints
- `config` — `ConcurrentKafkaListenerContainerFactory`, `KafkaListenerEndpointRegistry`, Java config support
- `core` — `KafkaTemplate` (send/receive), `KafkaAdmin` (topic management), `ProducerFactory`, `ConsumerFactory`
- `event` — Application events: `ListenerContainerIdleEvent`, `NonResponsiveConsumerEvent`, etc.
- `listener` — `KafkaMessageListenerContainer` (single-threaded), `ConcurrentMessageListenerContainer` (multi-threaded), error handlers, `AckMode`
- `mock` — `MockConsumer`/`MockProducer` wrappers for unit testing without a broker
- `requestreply` — `ReplyingKafkaTemplate` for request/reply messaging patterns
- `retrytopic` — Non-blocking retry with dead-letter topic routing (`@RetryableTopic`)
- `security` — OAuth/SASL token provider integration
- `streams` — `StreamsBuilderFactoryBean`, `@EnableKafkaStreams` for Kafka Streams support
- `support` — Converters, serializers, `KafkaHeaders`, `MessageConverter` hierarchy
- `transaction` — `KafkaTransactionManager`, transactional producer support

**Message conversion**: `MessageConverter` hierarchy — `MessagingMessageConverter` (default), `StringJsonMessageConverter`, `BytesJsonMessageConverter`.

**Listener containers**: Two main implementations — `KafkaMessageListenerContainer` (single consumer thread) and `ConcurrentMessageListenerContainer` (pool of `KafkaMessageListenerContainer` instances, one per partition or configurable concurrency). Both implement `MessageListenerContainer`.

**Error handling**: Pluggable via `CommonErrorHandler` — `DefaultErrorHandler` (with `BackOff` strategies), `CommonContainerStoppingErrorHandler`, and `DeadLetterPublishingRecoverer` for routing failed records.

## A Day in the Life of a Contributor

* _Always_ work on topic branches (typically use the GitHub issue ID as the branch name).
  - For example, to create and switch to a new branch for issue #123: `git checkout -b GH-123`
* You might be working on several different topic branches at any given time, but when at a stopping point for one of those branches, commit (a local operation).
* Please follow the "Commit Guidelines" described in [this chapter of Pro Git](https://git-scm.com/book/en/Distributed-Git-Contributing-to-a-Project).
* Then to begin working on another issue (say GH-101): `git checkout GH-101`. The _-b_ flag is not needed if that branch already exists in your local repository.
* When ready to resolve an issue or to collaborate with others, you can push your branch to origin (your fork),
  e.g.: `git push origin GH-123`
* If you want to collaborate with another contributor, have them fork your repository (add it as a remote) and `git fetch <your-username>` to grab your branch.
  Alternatively, they can use `git fetch --all` to sync their local state with all of their remotes.
* If you grant those collaborator(s) push access to your repository, they can even apply their changes to your branch.
* When ready for your contribution to be reviewed for potential inclusion in the main branch of the canonical spring-kafka repository (what you know as 'upstream'), issue a pull request to the SpringSource repository (for more detail, see [Using pull requests](https://help.github.com/articles/using-pull-requests/)).
* The project lead may merge your changes into the upstream main branch as-is, they may keep the pull request open yet add a comment about something that should be modified, or they might reject the pull request by closing it.
* A prerequisite for any pull request is that it will be cleanly merge-able with the upstream main's current state.
  **This is the responsibility of any contributor.**
  If your pull request cannot be applied cleanly, the project lead will most likely add a comment requesting that you make it merge-able.
  For a full explanation, see [the Pro Git section on rebasing](https://git-scm.com/book/en/Git-Branching-Rebasing).
  As stated there: _"> Often, you'll do this to make sure your commits apply cleanly on a remote branch — perhaps in a project to which you're trying to contribute but that you don't maintain."_

## Keeping your Local Code in Sync

* As mentioned above, you should always work on topic branches (since 'main' is a moving target). However, you do want to always keep your own 'origin' main branch in synch with the 'upstream' main.
* Within your local working directory, you can sync up all remotes' branches with: `git fetch --all`
* While on your own local main branch: `git pull upstream main` (which is the equivalent of fetching upstream/main and merging that into the branch you are in currently)
* Now that you're in synch, switch to the topic branch where you plan to work, e.g.: `git checkout -b GH-123`
* When you get to a stopping point: `git commit`
* If changes have occurred on the upstream/main while you were working you can synch again:
  - Switch back to main: `git checkout main`
  - Then: `git pull upstream main`
  - Switch back to the topic branch: `git checkout GH-123` (no -b needed since the branch already exists)
  - Rebase the topic branch to minimize the distance between it and your recently synched main branch: `git rebase main`
    (Again, for more detail see [the Pro Git section on rebasing](https://git-scm.com/book/en/Git-Branching-Rebasing)).
* **Note** While it is generally recommended to _not_ re-write history by using `push --force`, and we do not do this on `main` (and release) branches in the main repo, we require topic branches for pull requests to be rebased before merging, in order to maintain a clean timeline and avoid "merge" commits.
* If, while rebasing for the merge, we find significant conflicts, we may ask you to rebase and `push --force` to your topic branch after resolving the conflicts.
* Assuming your pull request is merged into the 'upstream' main, you will actually end up pulling that change into
  your own main eventually, and at that time, you may decide to delete the topic branch from your local repository and
  your fork (origin) if you pushed it there.
  - to delete the local branch: `git branch -d GH-123`
  - to delete the branch from your origin: `git push origin :GH-123`

## Maintain a linear commit history

When merging to main, the project _always_ uses fast-forward merges.
When issuing pull requests, please ensure that your commit history is linear.
From the command line you can check this using:

```
log --graph --pretty=oneline
```

As this may cause lots of typing, we recommend creating a global alias, e.g. `git logg` for this:

```
git config --global alias.logg 'log --graph --pretty=oneline'
```

This command will provide the following output, which in this case shows a nice linear history:

```
* e11821842f22b1a9461ab2209c0be7cd47f5c25e GH-123: Add feature X
* 59710724e5df4414139f67145152fc943d45baf7 GH-101: Fix NPE in KafkaTemplate
* d0f6e61308a020bdeb06992ece63fa3c4bad99d7 GH-99: Improve error handler backoff
```

If you see intersecting lines, that usually means that you forgot to rebase your branch.
As mentioned earlier, **please rebase against main** before issuing a pull request.

## Mind the whitespace

Please carefully follow the whitespace and formatting conventions already present in the framework.

1. Tabs, not spaces
2. Unix (LF), not DOS (CRLF) line endings
3. Eliminate all trailing whitespace
4. Wrap Javadoc at 90 characters
5. Aim to wrap code at 120 characters, but favor readability over wrapping
6. Preserve existing formatting; i.e. do not reformat code for its own sake
7. Search the codebase using `git grep` and other tools to discover common naming conventions, etc.
8. Latin-1 (ISO-8859-1) encoding for Java sources; use `native2ascii` to convert if necessary

Checkstyle is enforced at build time using the configuration in `src/checkstyle/`.

## Add Apache license header to all new classes

```java
/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ...;
```

## Use @since tags

Use @since tags for newly-added public API types and methods e.g.

```java
/**
 * ...
 *
 * @author First Last
 *
 * @since 3.0
 *
 * @see ...
 */
```

(Note the blank lines between tag sections for better readability.)

## Submit JUnit test cases for all behavior changes

Search the codebase to find related unit tests and add additional @Test methods within.
It is also acceptable to submit test cases on a per GitHub issue basis.

Most tests use `@EmbeddedKafka` (from `spring-kafka-test`) which starts an in-process KRaft broker — no external broker needed.
For tests that require a specific broker configuration, `EmbeddedKafkaBroker` / `EmbeddedKafkaKraftBroker` can be used directly.
TestContainers (`org.testcontainers:kafka`) is available for tests that need a real broker environment.

## Squash commits

Use `git rebase --interactive`, `git add --patch` and other tools to "squash" multiple commits into atomic changes.
In addition to the man pages for git, there are many resources online to help you understand how these tools work.
**Do not** squash commits after a PR is issued against the branch - especially after resolving issues from the review; just add more commits to the branch; it is easier to see what changed since the review.
The committer will generally squash the commits before the final merge.

## Use your real name in git commits

Please configure git to use your real first and last name for any commits you intend to submit as pull requests.
For example, this is not acceptable:

    Author: Nickname <user@mail.com>

Rather, please include your first and last name, properly capitalized:

    Author: First Last <user@mail.com>

This helps ensure traceability and also goes a long way to ensuring useful output from tools like `git shortlog` and others.

You can configure this globally via the account admin area GitHub (useful for fork-and-edit cases); globally with

    git config --global user.name "First Last"
    git config --global user.email user@mail.com

or locally for the **spring-kafka** repository only by omitting the '--global' flag:

    cd spring-kafka
    git config user.name "First Last"
    git config user.email user@mail.com

## Run all tests prior to submission

See the [checking out and building](https://github.com/spring-projects/spring-kafka#checking-out-and-building)
section of the README for instructions.
Make sure that all tests pass prior to submitting your pull request.

## Mention your pull request on the associated GitHub issue

Add a comment to the associated GitHub issue(s) linking to your new pull request.

## Provide a Link to the GitHub issue in the Associated Pull Request

Add a GitHub issue link to your first commit comment of the pull request, so your commit message
may look like this:

```
GH-123: Title up to 50 characters total

Fixes https://github.com/spring-projects/spring-kafka/issues/123

Text about the problem and how it was fixed.
```
