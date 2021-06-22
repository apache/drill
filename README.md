The Apache Drill website is built using [Jekyll](http://jekyllrb.com/), from Markdown sources in the
[drill/gh-pages](https://github.com/apache/drill/tree/gh-pages branch of the main Drill code repository.
Changes made anywhere downstream of that will be lost in the next build and deploy cycle.

To make documenation contributions easier, pull requests to the gh-pages branch do not require any
additional process, such as the creation of a JIRA ticket.

# Configuring env

1. Install `ruby`
2. Install `bundler` and `jekyll` `v3.9.0`:

```
gem install bundler jekyll:3.9.0
```

3. Install Jekyll plugins.

```
gem install jekyll-redirect-from:0.9.1
gem install jekyll-polyglot
```

5. Install Python 3

Please make sure that specific versions of libraries are installed since building the site with other versions
may cause some issues like including md document index into the references, etc.

## Note for existing contributors

The software version numbers above underwent a major increase in 2020 and the Markdown processor
changed from Redcarpet to Kramdown. Please check the versions in your environment if you're having
trouble generating the site.

# Documentation Guidelines

The documentation pages are placed under `_docs`. You can modify existing .md files, or you can create new .md files to add to the Apache Drill documentation site. Create pull requests to submit your documentation updates. The Kramdown Markdown processor employed by Jekyll supports [a dialect of Markdown](https://kramdown.gettalong.org/quickref.html) which is a superset of standard Markdown.

## Creating New Markdown Files

If you create new Markdown (.md) files, include the required YAML front matter and name the file using the methods described in this section.

The YAML front matter has three important parameters:

- `title:` - This is the title of the page enclosed in quotation marks. Each page must have a _unique_ title
- `slug:` - Set this to the same value as `title`, it will be slugified automatically by Jekyll.
- `date:` - This field is needed for Jekyll to write a last-modified date. Initially, leave this field blank.
- `parent:` - This is the title of the page's parent page. It should be empty for top-level sections/guides, and be identical to the title attribute of another page in all other cases.

The name of the file itself doesn't matter except for the alphanumeric order of the filenames. Files that
share the same parent are ordered alphanumerically. Note that the content of parent files is ignored, so add an
overview/introduction child when needed.

Best practices:

- Prefix the filenames with `010-foo.md`, `020-bar.md`, `030-baz.md`, etc. This allows room to add files in-between
  (eg, `005-qux.md`).
- Use the slugified title as the filename. For example, if the title is "Getting Started with
  Drill", name the file `...-getting-started-with-drill.md`. If you're not sure what the slug is, you should be
  able to see it in the URL and then adjust (the URLs are auto-generated based on the title attribute).

# Developing and Previewing the Website

To preview the website on your local machine:

```bash
jekyll build --config _config.yml,_config-prod.yml
_tools/createdatadocs.py
jekyll serve --config _config.yml,_config-prod.yml [--livereload] [--incremental]
```

Note that you can skip the first two commands (and only run `jekyll serve`) if you haven't changed the title or
path of any of the documentation pages.

## One Time Setup for Last-Modified-Date

To automatically add the `last-modified-on date`, a one-time local setup is required:

1.  In your cloned directory of Drill, in `drill/.git/hooks`, create a file named `pre-commit` (no extension) that contains this script:

```
#!/bin/sh
# Contents of .git/hooks/pre-commit

git diff --cached --name-status | grep "^M" | while read a b; do
  cat $b | sed "/---.*/,/---.*/s/^date:.*$/date: $(date -u "+%Y-%m-%d")/" > tmp
  mv tmp $b
  git add $b
done
```

2. Make the file executable.

```
chmod +x pre-commit
```

On the page you create, in addition to the title, and `parent:`, you now need to add `date:` to the front matter of any file you create. For example:

```
---
title: "Configuring Multitenant Resources"
parent: "Configuring a Multitenant Cluster"
date:
---
```

Do not fill in or alter the date: field. Jekyll and git take care of that when you commit the file.

## One Time Setup for Redirecting gh-pages

Locally install the `jekyll-redirect-from` gem:

```
gem install jekyll-redirect-from
```

On any page you want to redirect, add the redirect_to: and the URL to the front matter. For example:

```
---
title: "Configuring Multitenant Resources"
parent: "Configuring a Multitenant Cluster"
date:
redirect_to:
  - http://<new_url>
---
```
# Multilingual

Multilingual support was added to the docs section of the website in June 2021 using the polyglot Jekyll plugin.  The fallback language is set to English which means that when a translated page is not available the English version will be shown.

## Add a new language

1. Add the two-letter language code to the `languages` property in _config.yml.
2. Add a subdirectory with name equal to the language code under _docs/.

## Add translated documentation

1. Incrementally add translated pages to the relevant language subdirectory under _docs/ by replicating the path structure under _docs/en.
2. In the "front matter" at the top of each new page, translate both `title` and `parent` but leave the `slug` the same as the English page and set `lang` to the language code you are writing in.
3. A language which is incompletely translated is still deployable with no adverse effects (see the remark concerning English fallback above).

# Compiling the Website

Once the website is ready, you'll need to compile the site to static HTML so that it can then be published to Apache. This is as simple as running the `jekyll build` command. The `_config-prod.yml` configuration file causes a few changes to the site:

- The `noindex` meta tag is removed. We want the production site to be indexed by search engines, but we don't want the staging site to be indexed.
- The base URL is set to `/`. The production site is at `/`, whereas the staging site is at `/drill` (convenient for previewing on GitHub Pages: <http://apache.github.io/drill>).

```bash
jekyll build --config _config.yml,_config-prod.yml
_tools/createdatadocs.py
jekyll serve --config _config.yml,_config-prod.yml
```

# Uploading to the Apache Website (Drill Committers Only)

Apache project websites use a system called svnpubsub for publishing. Basically, the static HTML needs to be pushed by one of the committers into the Apache SVN.

```bash
git clone -b asf-site https://gitbox.apache.org/repos/asf/drill-site.git ../drill-site
rm -rf ../drill-site/*
cp -R _site/* ../drill-site/
cd ../drill-site
git status
git add *
git commit -m "Website update"
git push
```

The updates should then be live: <http://drill.apache.org>.
