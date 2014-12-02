The Apache Drill website is built using [Jekyll](http://jekyllrb.com/).

# Developing and Previewing the Website

The expectation was that we would be able to preview the website through GitHub Pages: <http://apache.github.io/drill>. For some reason, this is not working. Instead, you should fork the repository to your personal GitHub, then push some commit to the `gh-pages` branch in your fork, and you should be able to preview the site at <http://tshiran.github.io/drill> (replace `tshiran` with your own username, and `drill` with the name of the repostitory).

To preview the website on your local machine:

```bash
jekyll serve --config _config.yml,_config-prod.yml
```

# Compiling the Website

Once the website is ready, you'll need to compile the site to static HTML so that it can then be published to Apache. This is as simple as running the `jekyll build` command. The _config-prod.yml configuration file causes a few changes to the site:

* The `noindex` meta tag is removed. We want the production site to be indexed by search engines, but we don't want the staging site to be indexed.
* The base URL is set to `/`. The production site is at `/`, whereas the staging site is at `/drill-webiste/`.

```bash
jekyll build --config _config.yml,_config-prod.yml
```

# Uploading to the Apache Website (Drill Committers Only)

Apache project websites use a system called svnpubsub for publishing. Basically, the static HTML needs to be pushed by one of the committers into the Apache SVN.

```bash
svn co https://svn.apache.org/repos/asf/drill/site/trunk/content/drill ../_site-apache
cp -R _site/* ../_site-apache/
cd ../_site-apache
```

Then `svn add` and `svn rm` as needed, and commit the changes via `svn commit -m "Website update"`. Note that once changes are committed via `svn commit`, they will immediately be visible on the live site: <http://drill.apache.org>.
