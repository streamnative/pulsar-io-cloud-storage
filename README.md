## Pulsar IO :: Template

This is a template project for developing an enterprise-grade
pulsar IO connector.

This template already sets up a project structure, including
necessary dependencies and plugins. The IO connector developers
can clone this project to develop their own connector.

### Project Layout

Before starting developing your own connector, please take a look at
how this template project organize the files for a connector.

```bash

├── conf
├── docs
├── src
│   ├── checkstyle
│   ├── license
│   │   └── ALv2
│   ├── main
│   │   └── java
│   ├── spotbugs
│   └── test
│       └── java

```

- `conf` directory is used for storing examples of config files of this connector.
- `docs` directory is used for keeping the documentation of this connector.
- `src` directory is used for storing the source code of this connector.
  - `src/checkstyle`: store the checkstyle configuration files
  - `src/license`: store the license header for this project. `mvn license:format` can
    be used for formatting the project with the stored license header in this directory.
  - `src/spotbugs`: store the spotbugs configuration files
  - `src/main`: for keeping all the main source files
  - `src/test`: for keeping all the related tests

### Develop a Connector

Here are the instructions for developing a Pulsar connector `pulsar-io-foo`
use this project template.

#### Clone the Template

You can clone the template project with `--bare`.

```bash
$ git clone --bare https://github.com/streamnative/pulsar-io-template.git pulsar-io-foo
```

#### Push to Github

You can create a `pulsar-io-foo` project at your Github account and push the project to your
Github account.

```
$ cd pulsar-io-foo
$ git push https://github.com/<Your-Github-Account>/pulsar-io-foo
```

Once the project is pushed to your Github account, you can then develop the connector as
a normal Github project.

```bash
$ cd ..
$ rm -rf pulsar-io-foo
$ git clone https://github.com/<Your-Github-Account>/pulsar-io-foo
```

#### Update Pom File

The first thing to do is to update the [pom file](pom.xml) to customize your connector.

1. Change `artifactId` to `pulsar-io-foo`.
2. Update `version` to a version you like. A good practice is to pick the pulsar version
   as the same version for your connector so that it is easy to figure out what version of
   this connector works for what version of Pulsar.
3. Update `name` to `Pulsar Ecosystem :: IO Connector :: <Your Connector Name>`.
4. Update `description` to the description of your connector.

Once the above steps are done, you will have a real `pulsar-io-foo` project to develop
your own connector.

#### Implement Your Connector

Before you start implementing your own connector, it would be good to remove the example
connector included in the project template.

```bash
$ rm -rf src/main/java/org/apache/pulsar/ecosystem/io/random
```

Then you can create a package `org.apache.pulsar.ecosystem.io.foo` to develop your connector
logic under it.

#### Test Your Connector

It is strongly recommended to write tests for your connector.

There are a few test examples under
[src/test/java/org/apache/pulsar/ecosystem/io/random](src/test/java/org/apache/pulsar/ecosystem/io/random)
showing how to test a connector.

Before you start writing tests for your connector, you can remove those examples

```bash
$ rm -rf src/test/java/org/apache/pulsar/ecosystem/io/random
```

Then you can create a package `org.apache.pulsar.ecosystem.io.foo` under `src/test` directory
to develop the tests for your connector.

#### Checkstyle and Spotbugs

The template project already sets up checkstyle plugin and spotbugs plugin for ensuring you
write a connector that has a consistent coding convention with other connectors and high code
quality.

To run checkstyle:

```bash
$ mvn checkstyle:check
```

To run spotbugs:

```bash
$ mvn spotbugs:check
```

#### License

Before you publish your connector for others to use, you might consider pick up a license
you like to use for your connector.

Once you choose the license, you should do followings:

- Replace the `LICENSE` file with your chosen license.
- Add your license header to `src/license/<your-license-header>.txt`.
- Update the license-maven-plugin configuration in pom.xml to point to your license header
  `src/license/<your-license-header>.txt`.
- Run `license:format` to format the project with your license











