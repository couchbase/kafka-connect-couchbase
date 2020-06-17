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

Start by running `mvn clean verify -Prelease` to make sure the project builds successfully,
artifact signing works, and the unit tests pass.
When you're satisfied with the test results, it's time to...


## Bump the project version number

1. Edit `pom.xml` and remove the `-SNAPSHOT` suffix from the version string.
2. Edit `examples/custom-extensions/pom.xml` and update the `kafka-connect-couchbase.version` property.
3. Edit `README.md` and bump the version numbers if applicable.
4. Commit these changes, with message "Prepare x.y.z release"
(where x.y.z is the version you're releasing).


## Tag the release

Run the command `git tag -s x.y.z` (where x.y.z is the release version number).

Suggested tag message is "Release x.y.z".

Don't push the tag right away, though.
Wait until the release is successful and you're sure there will be no more changes.
Otherwise it can be a pain to remove an unwanted tag from Gerrit.


## Go! Go! Go!

Here it is, the moment of truth. When you're ready to deploy to the Maven Central Repository:

    mvn clean deploy -Prelease

Alternatively, if you prefer to inspect the staging repository and
[complete the release manually](https://central.sonatype.org/pages/releasing-the-deployment.html),
set this additional property:

    mvn clean deploy -Prelease -DautoReleaseAfterClose=false

Remember, you can add `-DskipITs` to either command to skip integration tests if appropriate.

If publishing to Maven Central was successful, you're ready to publish the distribution archive to S3 with this shell command:

    VERS=x.y.z
    ARTIFACT=couchbaseinc-kafka-connect-couchbase-${VERS}.zip
    aws s3 cp target/components/packages/${ARTIFACT} \
        s3://packages.couchbase.com/clients/kafka/${VERS}/${ARTIFACT} \
        --acl public-read

Whew, you did it! Or the build failed and you're looking at a cryptic error message, in which
case you might want to check out the Troubleshooting section below.

If the release succeeded, now's the time to publish the tag:

    git push origin x.y.z

## Prepare for next dev cycle

Increment the version number in `pom.xml` and restore the `-SNAPSHOT` suffix.
Update the `dcp.client.version` property in `integration-test/pom.xml` to refer to the
new snapshot version.
Commit and push to Gerrit. Breathe in. Breathe out.

## Publishing a snapshot

After every passing nightly build, a snapshot should be published to the Sonatype OSS snapshot repository by running this command:

    mvn clean deploy -Psnapshot

## Troubleshooting

* Take another look at the Prerequisites section. Did you miss anything?
* [This gist](https://gist.github.com/danieleggert/b029d44d4a54b328c0bac65d46ba4c65) has
some tips for making git and gpg play nice together.
* If deployment fails because the artifacts are missing PGP signatures, make sure your Maven
command line includes `-Prelease` (or `-Pstage`) when running `mvn deploy`.
Note that this is a *profile* so it's specified with `-P`.
