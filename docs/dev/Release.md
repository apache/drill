# Drill Release Process

1. ## Setup release final cut-off date
    1. Start mail thread on Apache Drill dev list.

        Example:

        Subject:
        ```
        [DISCUSS] Drill 1.17.0 release
        ```
        Body:
        ```
        Hi Drillers,

        It's been several months since the last release and it is time to do the next one. I am volunteering to be the release manager.

        If there are any issues on which work is in progress, that you feel we *must* include in the release, please post in reply to this thread. Based on your input we'll define release cut off date.

        Kind regards

        Thanks
        ```
    2. Gather a list of Jiras that need to be included in the release, discuss possible blockers.
    3. Identify the final cut-off date.
    4. Before starting the release send a reminder to the dev list that merge into Apache Drill master is forbidden during release.

        Example:
        ```
        Note for the committers:
        until the release is not over and Drill version is not changed to 1.17.0-SNAPSHOT, please do not push any
        changes into Drill master.
        ```
2. ## Setup environment:
    1. ### SVN
        1. Install subversion client (see instructions on http://subversion.apache.org/packages.html#osx for
         installing svn on different systems).
        2. Check that svn works:
        ```
        svn co https://dist.apache.org/repos/dist/release/drill ~/src/release/drill-dist
        ```
        You also need writable access to Apache SVN. (You need to be a PMC member for this).
    2. ### GPG key:
        You will need a GPG key set up. This key is needed for signing the release.
        1. Read [Apache docs for release signing](http://www.apache.org/dev/release-signing.html).
        2. Install the `gpg2` package on Ubuntu, or `gnupg` on MacOS.
        3. Generate GPG key using the **Apache email address** if it wasn't done before:
            1. Run `gpg --gen-key` and follow the instructions.
            2. Remember your passphrase.
            3. Save your key pair:
                ```
               gpg --output mygpgkey_pub.gpg --armor --export MY_KEY_ID
               gpg --output mygpgkey_sec.gpg --armor --export-secret-key MY_KEY_ID
               ```
        4. Or import existing GPG key pair if required:
            ```
            gpg --import mygpgkey_pub.gpg
            gpg --allow-secret-key-import --import mygpgkey_sec.gpg
            ```
        5. Have another committer signed your key (add to the trust chain).
            Apache advises to do it at 
            [key signing parties](https://www.apache.org/dev/release-signing.html#key-signing-party).
        6. Make sure the default key is the key generated using the Apache email.
        7. Publish your public key to a public server (e.g. http://pgp.surfnet.nl or http://pgp.mit.edu)
            using Web-UIs or CLI (for example using the following command:
            `gpg --keyserver hkp://pool.sks-keyservers.net --trust-model always --send-keys`).
        8. Add your key signature into the the `KEYS` file in the Drill sources repo in the project root directory.
            Instruction on how to create the key signature is embedded at the beginning of the `KEYS` file.
        9. Add your key signature into the the `KEYS` file in `https://dist.apache.org` (PMC permissions required for this):
            1. `svn co https://dist.apache.org/repos/dist/release/drill ~/src/release/drill-dist`.
            2. Update `KEYS` file with required changes.
            3. Commit changes: `svn ci -m "Add FirstName LastName's key to KEYS file"`.
    3. ### Setting up LDAP credentials
        1. Go to http://id.apache.org and log in using your Apache login and password.
        2. Scroll to the bottom to the 'SSH Key' entry and add the SSH key from your `~/.ssh/id_rsa.pub` file.
            If you don't have the SSH key, you should generate it by running `ssh-keygen`.
        3. Note that you can add more than one SSH key corresponding to multiple machines.
        4. Enter your Apache password and submit the changes.
        5. Verify that you can do an sftp to the Apache server by running the following: `sftp <username>@home.apache.org`.
    4. ### Setup Maven
        1. Apache's Maven repository access is documented here:
            http://www.apache.org/dev/publishing-maven-artifacts.html
            http://www.apache.org/dev/publishing-maven-artifacts.html#dev-env.
        2. For deploying Maven artifacts, you must add entries to your `~/.m2/settings.xml` file as described here:
            http://www.apache.org/dev/publishing-maven-artifacts.html#dev-env.
        3. In order to encrypt the LDAP password, see here:
            http://maven.apache.org/guides/mini/guide-encryption.html.
            The encrypted master password should be added to `~/.m2/security-settings.xml`.
        4. Add encrypted password to `settings.xml` and `security-settings.xml`:
            1. run `mvn --encrypt-master-password` and add an encrypted master password to `security-settings.xml` file;
            2. run `mvn --encrypt-password` and add an encrypted Apache LDAP password to `settings.xml` file.
    5. Check that `NOTICE` file in sources has the current copyright year.
    6. Make sure you are using JDK 8.
3. ## Manual Release process (this section is more for information how release process is performed, release manager should use `automated release process` described later).
    1. Setup GPG Password env variable:
        ```
        read -s GPG_PASSPHRASE
        ```
    2. Tell GPG how to read a password from your terminal:
        ```
        export GPG_TTY=$(tty)
        ```
    3. Clean up old release directory and clone a new Drill repository:
        ```
        cd ~/src/release && \
        rm -rf drill/ && \
        git clone git@github.com:apache/drill.git && \
        cd drill
        ```
    4. Checkout the current release:
        ```
        git checkout <git commit>
        ```
    5. Make sure build with tests succeeded along with license checks:
        ```
        mvn install -Drat.skip=false -Dlicense.skip=false
        ```
    6. Enforce memory options:
        ```
        MAVEN_OPTS='-Xmx4g -XX:MaxPermSize=512m'
        ```
    7. Clear any release history:
        ```
        mvn release:clean -Papache-release -DpushChanges=false -DskipTests
        ```
    8. Delete outgoing release tag if it exists:
        ```
        git push --delete origin drill-1.17.0
        git tag -d drill-1.17.0
        ```
    9. Do the release preparation:
        ```
        mvn -X release:prepare -Papache-release -DpushChanges=false -DskipTests -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE}  -DskipTests=true -Dmaven.javadoc.skip=false" -DreleaseVersion=1.17.0 -DdevelopmentVersion=1.18.0-SNAPSHOT -Dtag=drill-1.17.0
        ```
    10. Make sure to change Drill version to the proper one.
    11. Check that `target` folder contains the following files (with the correct version number):
        ```
        apache-drill-1.17.0-src.tar.gz 
        apache-drill-1.17.0-src.tar.gz.asc
        apache-drill-1.17.0-src.tar.gz.sha512
        apache-drill-1.17.0-src.zip
        apache-drill-1.17.0-src.zip.asc 
        apache-drill-1.17.0-src.zip.sha512
        ```
    12. Verify signature, ensure that GPG key for Apache was used (see details at
        https://www.apache.org/info/verification.html#CheckingSignatures) and checksum
        (see details at https://www.apache.org/info/verification.html#CheckingHashes):
        ```
        ./tools/release-scripts/checksum.sh target/apache-drill-1.17.0-src.tar.gz
        ./tools/release-scripts/checksum.sh target/apache-drill-1.17.0-src.zip
        ./tools/release-scripts/checksum.sh distribution/target/apache-drill-1.17.0.tar.gz
        ```
    13. You should see 2 new commits on top of the branch, for example:
        ```
        [maven-release-plugin] prepare release drill-1.17.0
        [maven-release-plugin] prepare for next development iteration
        ```
        Also, you should see a new `drill-1.17.0` tag.
        Push the new tag to your personal github repository:
        ```
        git push origin drill-1.17.0
        ```
    14. Perform the release but do the commits into personal repo
        (since Apache doesn't allow rollbacks on master if mistakes are made):
        ```
        mvn release:perform -DconnectionUrl=scm:git:git@github.com:vvysotskyi/drill.git -DskipTests -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE} -DskipTests=true -DconnectionUrl=scm:git:git@github.com:vvysotskyi/drill.git"
        ```
        If you want to additionally check resulting archives and jars, add `-Dmaven.deploy.skip=true` flag to avoid deploying jars to the Nexus repository:
        ```
        mvn release:perform -DconnectionUrl=scm:git:git@github.com:vvysotskyi/drill.git -DskipTests -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE} -DskipTests=true -DconnectionUrl=scm:git:git@github.com:vvysotskyi/drill.git -Dmaven.deploy.skip=true"
        ```
        After checks are performed, run this command without the flag.
    15. Deploy the release commit:
        ```
        git checkout drill-1.17.0
        mvn deploy -Papache-release -DskipTests -Dgpg.passphrase=${GPG_PASSPHRASE}
        ```
    16. Copy release files to a local release staging directory:
        ```
        cp ~/src/release/drill/target/target/checkout/apache-drill-1.17.0-src.tar.gz* ~/release/1.17.0-rc0/ && \ 
        cp ~/src/release/drill/target/target/checkout/apache-drill-1.17.0.zip* ~/release/1.17.0-rc0/ \ 
        cp ~/src/release/drill/target/checkout/distribution/target/apache-drill-1.17.0.tar.gz* ~/release/1.17.0-rc0/ \ 
        ```
    17. Check if the artifacts are signed properly:
        ```
        ./tools/release-scripts/checksum.sh ~/release/1.17.0-rc0/apache-drill-1.17.0-src.tar.gz
        ./tools/release-scripts/checksum.sh ~/release/1.17.0-rc0/apache-drill-1.17.0-src.zip
        ./tools/release-scripts/checksum.sh ~/release/1.17.0-rc0/apache-drill-1.17.0.tar.gz
        ```
    18. Copy release files to a directory on `home.apache.org` for voting:
        ```
        scp ~/release/1.17.0-rc0/* <username>@home.apache.org:~/public_html/drill/releases/1.17.0/rc0
        ```

4. ## Automated release process
    Run the following script:
    ```
    tools/release-scripts/release.sh
    ```
    The release script will push the maven artifacts to the Maven staging repo.
5. ## Publish release candidate and vote
    1. Go to the [Apache Maven staging repo](https://repository.apache.org/) and close the new jar release.
        This step is done in the Maven GUI. For detailed instructions on sonatype GUI please refer to 
        https://central.sonatype.org/pages/releasing-the-deployment.html#locate-and-examine-your-staging-repository.
    2. Start vote (vote should last at least 72 hours).

        Email should be sent to the Drill user and dev mailing lists.

        Example:

        Subject:
        ```
        [VOTE] Release Apache Drill 1.12.0 - RC0
        ```
        Body:
        ```
        Hi all,

        I'd like to propose the first release candidate (RC0) of Apache Drill, version 1.12.0.

        The release candidate covers a total of 100500 resolved JIRAs [1]. Thanks to everyone who contributed to this release.

        The tarball artifacts are hosted at [2] and the maven artifacts are hosted at [3].

        This release candidate is based on commit 54d3d201882ef5bc2e0f754fd10edfead9947b60 located at [4].

        Please download and try out the release.

        The vote ends at 5 PM UTC (9 AM PDT, 7 PM EET, 10:30 PM IST), January 1, 1970.

        [ ] +1
        [ ] +0
        [ ] -1

        Here's my vote: +1


        [1] https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12313820&version=12341087
        [2] http://home.apache.org/~arina/drill/releases/1.12.0/rc0/
        [3] https://repository.apache.org/content/repositories/orgapachedrill-1043/
        [4] https://github.com/arina-ielchiieva/drill/commits/drill-1.12.0
        ```
    3. If the vote fails, cancel RC and prepare new RC:
        1. Send an email with announcing about canceling the vote.

            Example:

            Subject:
            ```
            [CANCEL] [VOTE] Release Apache Drill 1.17.0 - RC0
            ```
            Body:
            ```
            Hi all,

            The vote for Apache Drill 1.17.0 - RC0 was canceled due to
            [list issues]

            Thanks to all who voted. A new release will be tagged as 1.17.0 - RC1 and
            will be available for voting soon.
            ```
        2. Go to the [Apache Maven staging repo](https://repository.apache.org/) and drop the jar release.
        3. Delete the release tag locally and remotely:
            ```
            git push --delete origin drill-1.17.0
            git tag -d drill-1.17.0
            ```
    4. If the vote passes, finish the release:
        1. Count votes (email to the dev mailing list).

            Example:

            Subject:
            ```
            [RESULT] [VOTE] Release Apache Drill 1.12.0 RC1
            ```
            Body:
            ```
            The vote passes. Thanks to everyone who has tested the release candidate and given their comments and votes. Final tally:

            3x +1 (binding): Arina, Aman, Parth

            5x +1 (non-binding): Vitalii, Holger, Prasad, Vova, Charles 

            No 0s or -1s.

            I'll start process for pushing the release artifacts and send an announcement once propagated.

            Kind regards
            ```
        2. Add the release to the [dist.apache.org](https://dist.apache.org/repos/dist/release/drill/) and delete the old version, keeping two most recent.
            This can only done by a PMC member:
            ```
            svn co https://dist.apache.org/repos/dist/release/drill ~/src/release/drill-dist
            cd ~/src/release/drill-dist
            mkdir drill-1.17.0
            cp -r ~/release/1.17.0-rc0 drill-1.17.0
            svn add drill-1.17.0
            svn commit --message "Upload Apache Drill 1.17.0 release."
            svn delete 1.15.0
            svn commit --message "Deleting drill-1.15.0 to keep only last two versions"
            ```
        3. Go to the [Apache Maven staging repo](https://repository.apache.org/) and promote the release to the production.
        4. Create branch and tag for this release and update Drill version in master (if used automated scripts, tag will be like this `drill-1.11.0`. Branch should be named as `1.11.0`).
            Add release description in GitHub [Drill releases page](https://github.com/apache/drill/releases):
            open release tag, press `Edit tag` and add Release title like `Apache Drill 1.17.0` and specify release description.
        5. After new tag is pushed, check that DockerHub triggered job to build and publish new image for the release to https://hub.docker.com/repository/docker/apache/drill.
        5. Wait 24 hours, check download mirrors.
        6. Post release:
            1. "What's New" for the new release.
            2. Update Apache JIRA and add release date for this release. Add a new release tag if not already there.
            3. Update Drill Web site:
                1. Generate release notes for Drill: https://confluence.atlassian.com/jira/creating-release-notes-185729647.html
                    and create a MarkDown file for the release notes - post to the site the day of the release.
                2. Write the blog post and push it out to the Apache Drill website so it can be referenced in the announcement.
                    As an example, may be used [this commit](https://github.com/apache/drill/commit/374f55c619f5b7aea5d5161ea21b6e341a682c3f#diff-dfd4d960a68ac8b9a8d6af52c1bcbe4a).
                3. Update the data in `_data/version.json` - making sure to either match your release notes url to the one
                    below (`docs/apache-drill-1-6-0-release-notes`) or update it with the different URL:
                    ```
                    {
                      "display_version": "1.6",
                      "full_version": "1.6.0",
                      "release_date": "February 16, 2016",
                      "blog_post":"/blog/2016/02/16/drill-1.6-released",
                      "release_notes": "https://drill.apache.org/docs/apache-drill-1-6-0-release-notes/"
                    }
                    ```
                    As an example, may be used [this commit](https://github.com/apache/drill/commit/374f55c619f5b7aea5d5161ea21b6e341a682c3f#diff-25d705dfbe4caa6041b61ece80270bc1)
                4. Build and publish JavaDocs to the Drill site:
                    1. Checkout to the release version: `git checkout drill-1.17.0`
                    2. Run the following command to generate Javadocs in `target/site/apidocs` directory:
                        ```
                        mvn install javadoc:aggregate -DskipTests
                        ```
                    3. Checkout to the `gh-pages` branch where sources for site are placed:
                        ```
                        git checkout gh-pages
                        ```
                    4. Remove JavaDocs for previous version:
                        ```
                        rm -rf apidocs
                        ```
                    5. Copy new JavaDocs and commit changes:
                        ```
                        mv target/site/apidocs .
                        ```
                    6. Commit changes with the commit message `Publish JavaDocs for the Apache Drill 1.17.0`
                5. Instructions how to build and deploy Web site may be found here:
                    https://github.com/apache/drill/blob/gh-pages/README.md
            3. Post the announcement about new release on [Apache Drill Twitter](https://twitter.com/apachedrill]).
            4. A PMC member needs to update the release date for new release here:
                https://reporter.apache.org/addrelease.html?drill
            5. Send the announcement to the `dev@drill.apache.org` and `announce@apache.org` mailing lists.

                Example:

                Subject:
                ```
                [ANNOUNCE] Apache Drill 1.17.0 Released
                ```
                Body:
                ```
                On behalf of the Apache Drill community, I am happy to announce the release of Apache Drill 1.17.0.

                Drill is an Apache open-source SQL query engine for Big Data exploration.
                Drill is designed from the ground up to support high-performance analysis
                on the semi-structured and rapidly evolving data coming from modern Big
                Data applications, while still providing the familiarity and ecosystem of
                ANSI SQL, the industry-standard query language. Drill provides
                plug-and-play integration with existing Apache Hive and Apache HBase
                deployments.

                For information about Apache Drill, and to get involved, visit the project website [1].

                Total of 200 JIRA's are resolved in this release of Drill with following
                new features and improvements [2]:

                - Hive complex types support (DRILL-7251, DRILL-7252, DRILL-7253, DRILL-7254)
                - ESRI Shapefile (shp) (DRILL-4303) and Excel (DRILL-7177) format plugins support
                - Drill Metastore support (DRILL-7272, DRILL-7273, DRILL-7357)
                - Upgrade to HADOOP-3.2 (DRILL-6540)
                - Schema Provision using File / Table Function (DRILL-6835)
                - Parquet runtime row group pruning (DRILL-7062)
                - User-Agent UDFs (DRILL-7343)
                - Canonical Map<K,V> support (DRILL-7096)
                - Kafka storage plugin improvements (DRILL-6739, DRILL-6723, DRILL-7164, DRILL-7290, DRILL-7388)

                For the full list please see release notes [3].

                The binary and source artifacts are available here [4].

                Thanks to everyone in the community who contributed to this release!

                1. https://drill.apache.org/
                2. https://drill.apache.org/blog/2019/12/26/drill-1.17-released/
                3. https://drill.apache.org/docs/apache-drill-1-17-0-release-notes/
                4. https://drill.apache.org/download/
                ```
