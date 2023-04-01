<p align="center">
  <a href="https://comunica.dev/">
    <img alt="Comunica" src="https://comunica.dev/img/comunica_red.svg" width="200">
  </a>
</p>

<p align="center">
  <strong>Incremental evaluation for Comunica</strong>
</p>

<p align="center">
<a href="https://github.com/maartyman/incremunica/actions?query=workflow%3ACI"><img src="https://github.com/maartyman/incremunica/workflows/CI/badge.svg" alt="Build Status"></a>
<a href="https://coveralls.io/github/maartyman/incremunica?branch=master"><img src="https://coveralls.io/repos/github/maartyman/incremunica/badge.svg?branch=master" alt="Coverage Status"></a>
</p>


This is a monorepo that builds upon the core comunica packages to allow for incremental query evaluation. 

## Development Setup

_(JSDoc: https://maartyman.github.io/incremunica/)_

This repository should be used by Comunica module **developers** as it contains multiple Comunica modules that can be composed.
This repository is managed as a [monorepo](https://github.com/babel/babel/blob/master/doc/design/monorepo.md)
using [Lerna](https://lernajs.io/).

If you want to develop new features
or use the (potentially unstable) in-development version,
you can set up a development environment for Comunica.

Comunica requires [Node.JS](http://nodejs.org/) 8.0 or higher and the [Yarn](https://yarnpkg.com/en/) package manager.
Comunica is tested on OSX, Linux and Windows.

This project can be setup by cloning and installing it as follows:

```bash
$ git clone https://github.com/maartyman/incremunica.git
$ cd incremunica
$ yarn install
```

**Note: `npm install` is not supported at the moment, as this project makes use of Yarn's [workspaces](https://yarnpkg.com/lang/en/docs/workspaces/) functionality**

This will install the dependencies of all modules, and bootstrap the Lerna monorepo.

Furthermore, this will add [pre-commit hooks](https://www.npmjs.com/package/pre-commit)
to build, lint and test.
These hooks can temporarily be disabled at your own risk by adding the `-n` flag to the commit command.

## License
This code is copyrighted by [Ghent University – imec](http://idlab.ugent.be/)
and released under the [MIT license](http://opensource.org/licenses/MIT).
