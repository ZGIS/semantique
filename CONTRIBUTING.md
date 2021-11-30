# Contributing

Thanks for taking the time to contribute to semantique! Contributions are welcome and greatly appreciated. The following is a set of guidelines for contributing. These are mostly guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request.

This project and everyone participating in it is governed by a [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to the package maintainers.

Contributions can come in many different forms, as we will outline below. You don't even have to know Python to be able to contribute!

## Creating issues and discussions

Opening [issues](https://github.com/ZGIS/semantique/issues) and [discussions](https://github.com/ZGIS/semantique/discussions) is also a form of contributing. They help us to improve the quality and user-friendliness of the package, as well as to build a community around the package.

Use **Issues** if ..

- .. you are using the package and something is not working as it should. In that case, use the [bug report template](https://github.com/ZGIS/semantique/issues/new?assignees=&labels=bug+%3Abug%3A&template=bug_report.md&title=). Please first check if its really a bug in `semantique`, and if there are not already open issues reporting the same bug.
- .. you have a request for a new feature. In that case, use the [feature request template](https://github.com/ZGIS/semantique/issues/new?assignees=&labels=feature+%3Agift%3A&template=feature_request.md&title=). Please first check if the feature is not already present, and if there are not already open issues requesting the same feature. A feature request is meant to be very specific. For broader ideas on improving the package, use *Discusssions* instead.

Use **Discussions** if ..

- .. you have questions about the package and its functionalities. There might always be someone in the community who is able to help you. Use the [Q&A tag](https://github.com/ZGIS/semantique/discussions/categories/q-a) for this purpose, and don't forget to mark an answer as *accepted* if it was helpful.
- .. you have broader ideas about improving the package and want to share them, such that they can be discussed with other members of the community. Use the [Ideas tag](https://github.com/ZGIS/semantique/discussions/categories/ideas) for that purpose. For very specific feature requests, use *Issues* instead.
- .. you have used the package in a cool application and want to share that with other members of the community, such that they can learn from it or give feedback. Use the [Show and Tell tag](https://github.com/ZGIS/semantique/discussions/categories/show-and-tell) for this purpose.

## Solving issues

If you know how to write Python you are welcome to contribute by solving open issues, for example by implementing new [features](https://github.com/ZGIS/semantique/labels/feature%20%3Agift%3A) or fixing [bugs](https://github.com/ZGIS/semantique/labels/bug%20%3Abug%3A). Especially those issues labelled with [help wanted](https://github.com/ZGIS/semantique/labels/help%20wanted%20%3Asos%3A) are desperately begging for contribution. Solving issues does not always involve writing code, you can also help by improving and extending [documentation](https://github.com/ZGIS/semantique/labels/documentation%20%3Abooks%3A).

In any case, the common code contributing workflow is:

#### 1. Cloning the GitHub repo

Clone this GitHub repo, or alternatively first fork it and then clone your forked version of the repo. After cloning, enter the cloned directory.

```
git clone git@github.com:ZGIS/semantique.git
cd semantique
```

#### 2. Creating a local development environment

Always create a new git branch to work in, instead of working in the main branch (if you have forked the repo first this is less important).

```
git checkout -b my_new_branch
```

If you work with conda, it can be helpful to create a new conda environment containing all the dependencies of semantique, without disturbing your base environment. To do so, you can make use the [environment.yml](environment.yml) file contained in this repo.

```
conda env create -f environment.yml
conda activate semantique
```

#### 3. Writing code

When writing code we try to follow the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html). However, we indent code blocks with **2 spaces** instead of 4 spaces. Don't forget to actually test your code before commiting.

#### 4. Commiting code to your local branch

When commiting changes with `git commit` we try to use structured commit messages, adapted from https://www.conventionalcommits.org/. The first line of commit message should have the following format: 

```
<type>: <summary>
```

The summary should be short (preferably < 50 characters), starting with an upper case, and written in present tense. If the commit references a specific issue, include `Refs #<issue number>` in the summary. If the issue is a bug report, you may also use `Fix #<issue number>` such that the issue gets closed automatically.

The type should be one of the defined types listed below. If you feel artistic, you can end the commit message with the emoji belonging to the type :sunglasses:.

- **feat**: Implementation of a new feature. Emoji `:gift:` :gift:
- **fix**: A bug fix. Emoji `:wrench:` :wrench:
- **style**: Changes to code formatting. No change to program logic. Emoji `:art:` :art:
- **refactor**: Changes to code which do not change behaviour, e.g. renaming variables or splitting functions. Emoji `:construction:` :construction:
- **docs**: Adding, removing or updating user documentation or to code comments. Emoji `:books:` :books:
- **logs**: Adding, removing or updating log messages. Emoji `:sound:` :sound:
- **test**: Adding, removing or updating tests. No changes to user code. Emoji `:test_tube:` :test_tube:
- **cicd**: Adding, removing or updating CI/CD workflows. No changes to user code. Emoji `:robot:` :robot:
- **deps**: Adding, removing or updating dependencies. Emoji `:couple:` :couple:
- **release**: Preparing a release, e.g. updating version numbers. Emoji `:bookmark` :bookmark:
- **repo**: Changes to the repository that do not involve code/documentation, e.g. adding templates or community files. Emoji `:package:` :package:

Example commit messages are:

```
git commit -m 'feat: Add bar parameter to foo(), Refs #10 :gift:'
git commit -m 'fix: Include type checking in foo(), Fix #12 :wrench:'
```

#### 5. Pushing your branch to the GitHub repo

Please **never push directly to the main branch**!

```
git push origin my_new_branch
```

#### 6. Creating a pull request

Create a request to merge your changes into the main branch using the [Pull Request](https://github.com/ZGIS/semantique/pulls) functionality from GitHub. This should automatically provide you with the [pull request template](https://github.com/ZGIS/semantique/blob/main/.github/pull_request_template.md). Add at least one of the package maintainer as reviewer of your pull request, and make sure the automatic checks done by GitHub pass without errors.

Happy coding!
