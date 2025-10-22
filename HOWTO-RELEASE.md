# Release Instructions

This is a guide for Couchbase employees.
It describes how to cut a release of this project and publish it to the Maven Central Repository as well as the public Couchbase S3 bucket.

## Before you start

### Check lastest snapshot

Check the **Maven Deploy Snapshot** GHA workflow and verify the lastest snapshot was published successfully. https://github.com/couchbase/kafka-connect-couchbase/actions/workflows/deploy-snapshot.yml

If there were issues with the workflow, resolve them before proceeding with the release.
Note that if you have to change `deploy-snapshot.yml`, you might also need to make corresponding changes to `deploy-release.yml`.

### Use a clean, up-to-date workspace

Make sure your local repository is up-to-date:

    git pull --rebase --autostash

Make sure you don't have any uncommitted files in your workspace; `git status` should say "nothing to commit, working tree clean".

## Refresh the generated documentation

Some reference documentation is generated from Javadoc.

Before you start, make sure you have cloned the connector documentation repository: https://github.com/couchbase/docs-kafka

Set the `KAFKAC_DOCS_REPO` environment variable to the path to the root directory of that repo.


Run this command to regenerate the docs:

    ./mvnw clean test-compile exec:java

If any AsciiDoc files in the docs repo are modified as a result, make sure the changes look good and then commit them.

## Update documentation

In the `docs-kafka` repo:

1. If releasing a new minor version, edit `docs/antora.yml` and update the version number.
2. Verify `compatibility.adoc` is up-to-date.

## Bump the project version number

1. Edit `pom.xml` and remove the `-SNAPSHOT` suffix from the version string.
2. Edit `examples/custom-extensions/pom.xml` and update the `kafka-connect-couchbase.version` property.
3. Edit `README.md` and bump the version numbers if applicable.
4. For major or minor version bumps, review uncommitted API and consider promoting to committed.
5. Verify the connector metadata `pom.xml` is up-to-date, particularly the `<requirements>` section. 
6. Commit these changes, with message "Prepare x.y.z release"
(where x.y.z is the version you're releasing).

NOTE: It's normal for the **Maven Deploy Snapshot** workflow to fail at this point because the project version no longer ends with `-SNAPSHOT`.


## Tag the release

After submitting the version bump change in Gerrit, run `git pull` so your local repo matches what's in Gerrit.

Run the command `git tag -s x.y.z` (where x.y.z is the release version number).
Suggested tag message is "Release x.y.z".

It's hard to change a tag once it has been pushed to Gerrit, so at this point you might want to do any final sanity checking.
At a minimum, make sure `./mvnw clean package` succeeds.


Push the tag by running the command:

    git push origin x.y.z


## Trigger the release workflow

Go to the **Maven Deploy Release** GitHub Action:

https://github.com/couchbase/kafka-connect-couchbase/actions/workflows/deploy-release.yml

Press the "Run workflow" button.
Leave "Use workflow from" at the default of "Branch: master".
Input the name of the tag you created in the previous step.

Take a deep breath, then push the "Run workflow" button to start the release process.

The workflow publishes the Couchbase Kafka connector API library to Maven Central, then uploads the connector distribution to our public S3 bucket.
If it succeeds, you should be able to download the connector distribution and its GPG signature from here:

    https://packages.couchbase.com/clients/kafka/<VERSION>/couchbase-kafka-connect-couchbase-<VERSION>.zip
    https://packages.couchbase.com/clients/kafka/<VERSION>/couchbase-kafka-connect-couchbase-<VERSION>.zip.asc


## Publish to Confluent Hub

This is a manual process.
Send a polite email to our friends at Confluent.
Let them know about the new download links for the connector and the GPG signature.

## Prepare for next dev cycle

Increment the version number in `pom.xml` and restore the `-SNAPSHOT` suffix.
Commit and push to Gerrit.

## Update Black Duck scan configuration

Clone the build-tools repository https://review.couchbase.org/admin/repos/build-tools

Edit `blackduck/couchbase-connector-kafka/scan-config.json`

Keep the `"master"` entry.
If you live in the future, also keep any entries for actively maintained release branches (for example, `"4.3.x"`).

Remove any entries for specific versions from the previous release cycle, and replace them with the version(s) you just released.

Commit the change.

NOTE: Entries for tagged versions only need to remain in this file long enough for Black Duck to scan the repository at least once.
