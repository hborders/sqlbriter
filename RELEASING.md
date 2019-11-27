Releasing
========

 1. Change the version in `gradle.properties` to a non-SNAPSHOT version.
 2. Update the `CHANGELOG.md` for the impending release.
 3. Update the `README.md` with the new version.
 4. `git commit -am "Prepare for release X.Y.Z."` (where X.Y.Z is the new version)
 5.
 [Create GPG key](https://proandroiddev.com/publishing-a-maven-artifact-3-3-step-by-step-instructions-to-mavencentral-publishing-bd661081645d)
 ```
 > gpg --full-generate-key # the defaults are certainly ok
                           # choose a strong password and note itpub   rsa2048 2019-05-30 [SC]
 # this is your key Id. This is public, you can share it (and should)
 80D5D092EA7F0F3F374AB28F67328B201D9BB9FE
 uid
                       Martin Bonnin <martin@martin.com>
 sub   rsa2048 2019-05-30 [E]# export and backup your secret key somewhere safe
 > gpg --export-secret-keys 1D9BB9FE > sonatype_upload.key# upload your public key to a public server so that sonatype can find it
 > gpg --keyserver hkp://pool.sks-keyservers.net --send-keys 1D9BB9FE
 ```

 ```
 ./gradlew clean uploadArchives \
 -Psigning.keyId=<your GPG signing key last-8-characters> \
 -Psigning.secretKeyRingFile=<full path to your sonatype_upload.key> \
 -Psigning.password=<your GPG password> \
 -PSONATYPE_NEXUS_USERNAME=<your sonatype jira/ossrh username>  \
 -PSONATYPE_NEXUS_PASSWORD=<your sonatype jira/ossrh password> \
 ```
 6. Visit [Sonatype Nexus](https://oss.sonatype.org/) and promote the artifact.
 7. `git tag -a X.Y.Z -m "Version X.Y.Z"` (where X.Y.Z is the new version)
 8. Update the `gradle.properties` to the next SNAPSHOT version.
 9. `git commit -am "Prepare next development version."`
 10. `git push && git push --tags`

If step 5 or 6 fails, drop the Sonatype repo, fix the problem, commit, and start again at step 5.
