# Release Instructions

This is a guide for Couchbase employees.
It describes how to cut a release of this project and publish it to the Maven Central Repository as well as the public Couchbase S3 bucket.


## Prerequisites

You will need:
* AWS credentials with write access to the `packages.couchbase.com` S3 bucket.
* The [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/), for uploading the distribution archive to S3.
* The `gpg` command-line program and a PGP key. Mac users, grab `gpg` from
https://gpgtools.org and enjoy
[this setup guide](http://notes.jerzygangi.com/the-best-pgp-tutorial-for-mac-os-x-ever/).
Generate a PGP key with your `couchbase.com` email address and uploaded it
to a public keyserver. For bonus points, have a few co-workers sign your key.
* To tell git about your signing key: `git config --global user.signingkey DEADBEEF`
(replace `DEADBEEF` with the id of your PGP key).
* A Sonatype account authorized to publish to the `com.couchbase` namespace.
* A `~/.m2/settings.xml` file with a
[User Token](https://blog.sonatype.com/2012/08/securing-repository-credentials-with-nexus-pro-user-tokens/).
To generate a token, go to https://oss.sonatype.org and log in with your Sonatype account.
Click on your username (upper right) and select "Profile". From the drop-down menu,
select "User Token". Press "Access User Token" to see a snippet of XML with server credentials.
Copy and paste this XML into the `servers` section of your Maven settings,
replacing `${server}` with `ossrh`.
* A local Docker installation, if you wish to run the integration tests.

At a minimum, your `~/.m2/settings.xml` should look something like:

    <settings>
      <servers>
        <server>
          <id>ossrh</id>
          <username>xxxxxxxx</username>
          <password>xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx</password>
        </server>
      </servers>
    </settings>

All set? In that case...


## Let's do this!

Start by running `./mvnw clean verify -Prelease` to make sure the project builds successfully,
artifact signing works, and the unit tests pass.
When you're satisfied with the test results, it's time to...

## Refresh the generated documentation

Some reference documentation is generated from Javadoc.

Before you start, make sure you have cloned the connector documentation repository: https://github.com/couchbase/docs-kafka

Set the `KAFKAC_DOCS_REPO` environment variable to the path to the root directory of that repo.


Run this command to regenerate the docs:

    ./mvnw clean test-compile exec:java

If any AsciiDoc files in the docs repo are modified as a result, make sure the changes look good and then commit them.

## Update documentation

In the `docs-kafka` repo:

1. Edit `docs/antora.yml` and bump the version number if application.
2. Verify `compatibility.adoc` is up-to-date.

## Bump the project version number

1. Edit `pom.xml` and remove the `-SNAPSHOT` suffix from the version string.
2. Edit `examples/custom-extensions/pom.xml` and update the `kafka-connect-couchbase.version` property.
3. Edit `README.md` and bump the version numbers if applicable.
4. For major or minor version bumps, review uncommitted API and consider promoting to committed.
5. Verify the connector metadata `pom.xml` is up-to-date, particularly the `<requirements>` section. 
6. Commit these changes, with message "Prepare x.y.z release"
(where x.y.z is the version you're releasing).


## Tag the release

Run the command `git tag -s x.y.z` (where x.y.z is the release version number).

Suggested tag message is "Release x.y.z".

Don't push the tag right away, though.
Wait until the release is successful and you're sure there will be no more changes.
Otherwise it can be a pain to remove an unwanted tag from Gerrit.


## Go! Go! Go!

Make sure you don't have any uncommitted files in your workspace:

    git status

should say "nothing to commit, working tree clean".

Here it is, the moment of truth. When you're ready to deploy to the Maven Central Repository:

    ./mvnw clean deploy -Prelease

Alternatively, if you prefer to inspect the staging repository and
[complete the release manually](https://central.sonatype.org/pages/releasing-the-deployment.html),
set this additional property:

    ./mvnw clean deploy -Prelease -DautoReleaseAfterClose=false

Remember, you can add `-DskipITs` to either command to skip integration tests if appropriate.

If publishing failed, see the troubleshooting section below.

If publishing to Maven Central was successful, you're ready to publish the distribution archive to S3 with this shell command:

    VERS=x.y.z
    ARTIFACT=couchbase-kafka-connect-couchbase-${VERS}.zip
    aws s3 cp target/components/packages/${ARTIFACT} \
        s3://packages.couchbase.com/clients/kafka/${VERS}/${ARTIFACT} \
        --acl public-read

    gpg --detach-sign --armor target/components/packages/${ARTIFACT}
    aws s3 cp target/components/packages/${ARTIFACT}.asc \
            s3://packages.couchbase.com/clients/kafka/${VERS}/${ARTIFACT}.asc \
            --acl public-read

Whew, you did it! Or the build failed and you're looking at a cryptic error message, in which
case you might want to check out the Troubleshooting section below.

If the release succeeded, now's the time to publish the tag:

    git push origin x.y.z

## Publish to Confluent Hub

This is a manual process.
Send a polite email to our friends at Confluent.
Let them know about the new download links for the connector and the GPG signature.

## Prepare for next dev cycle

Increment the version number in `pom.xml` and restore the `-SNAPSHOT` suffix.
Commit and push to Gerrit. Breathe in. Breathe out.

## Update Black Duck scan configuration

Clone the build-tools repository http://review.couchbase.org/admin/repos/build-tools

Edit `blackduck/couchbase-connector-kafka/scan-config.json`

Copy and paste the latest version entry; update it to refer to the version under development. For example, if you just bumped the version to 4.3.2-SNAPSHOT, the new version you're adding here should be "4.3.2"

Commit the change.

## Publishing a snapshot

After every passing nightly build, a snapshot should be published to the Sonatype OSS snapshot repository by running this command:

    ./mvnw clean deploy -Psnapshot

## Troubleshooting

* Take another look at the Prerequisites section. Did you miss anything?
* [This gist](https://gist.github.com/danieleggert/b029d44d4a54b328c0bac65d46ba4c65) has
some tips for making git and gpg play nice together.
* If deployment fails because the artifacts are missing PGP signatures, make sure your Maven
command line includes `-Prelease` when running `mvn deploy`.
Note that this is a *profile* so it's specified with `-P`.

### Maven Central timeouts

Did you get an error like this? 

```
[INFO] Remote staged 1 repositories, finished with success.
[INFO] Remote staging repositories are being released...

Waiting for operation to complete...
.......Jan 26, 2024 1:47:55 PM com.sun.jersey.api.client.ClientResponse getEntity
SEVERE: A message body reader for Java class com.sonatype.nexus.staging.api.dto.StagingProfileRepositoryDTO, and Java type class com.sonatype.nexus.staging.api.dto.StagingProfileRepositoryDTO, and MIME media type text/html was not found
```

It might just mean the Sonatype server is having a busy day. 
It might have published the connector eventually, but not before the client timed out.
Log in to https://oss.sonatype.org and inspect the released connector versions at
https://oss.sonatype.org/#nexus-search;quick~kafka-connect-couchbase

If you don't see the new version listed there, then publication probably failed.
Try the Maven command again.

If you see the new version there, the release probably succeeded.
Wait several minutes, then verify the new version appears in Maven Central at
https://repo1.maven.org/maven2/com/couchbase/client/kafka-connect-couchbase/
