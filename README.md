The Apache Drill website is built using [Jekyll](http://jekyllrb.com/).

# Developing and Previewing the Website

To preview the website on your local machine:

```bash
jekyll build --config _config.yml,_config-prod.yml
_tools/createdatadocs.py
jekyll serve --config _config.yml,_config-prod.yml
```
Note that you can skip the first two commands (and only run `jekyll serve`) if you haven't changed the title or path of any of the documentation pages.

# Compiling the Website

Once the website is ready, you'll need to compile the site to static HTML so that it can then be published to Apache. This is as simple as running the `jekyll build` command. The _config-prod.yml configuration file causes a few changes to the site:

* The `noindex` meta tag is removed. We want the production site to be indexed by search engines, but we don't want the staging site to be indexed.
* The base URL is set to `/`. The production site is at `/`, whereas the staging site is at `/drill` (convenient for previewing on GitHub Pages: <http://apache.github.io/drill>).

```bash
jekyll build --config _config.yml,_config-prod.yml
_tools/createdatadocs.py
jekyll build --config _config.yml,_config-prod.yml
```

# Uploading to the Apache Website (Drill Committers Only)

Apache project websites use a system called svnpubsub for publishing. Basically, the static HTML needs to be pushed by one of the committers into the Apache SVN.

```bash
git clone -b asf-site https://git-wip-us.apache.org/repos/asf/drill-site.git ../drill-site
cp -R _site/* ../drill-site/
cd ../drill-site
git status
git add *
git commit -m "Website update"
git push
```

The updates should then be live: <http://drill.apache.org>.

# Documentation Guidelines

The documentation pages are under `_docs`. Each file, in its YAML front matter, has two important parameters:

* title - This is the title of the page. Each page must have a *unique* title
* parent - This is the title of the page's parent page. It should be empty for top-level sections/guides, and be identical to the title attribute of another page in all other cases.

The name of the file itself doesn't matter except for the alphanumeric order of the filenames. Files that share the same parent are ordered alphanumerically. Note that the content of parent files is ignored, so add an overview/introduction child when needed.

Best practices:

* Prefix the filenames with `010-foo.md`, `020-bar.md`, `030-baz.md`, etc. This allows room to add files in-between (eg, `005-qux.md`).
* Use the slug of the title as the filename. For example, if the title is "Getting Started with Drill", name the file `...-getting-started-with-drill.md`. If you're not sure what the slug is, you should be able to see it in the URL and then adjust (the URLs are auto-generated based on the title attribute).
