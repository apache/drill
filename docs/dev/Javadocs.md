# Javadocs

## Building

 1. Make sure you are in drill's root project directory.
 1. Build the project:
    ```
    mvn -T 1C clean install -DskipTests
    ```
 1. Run:
    ```
    mvn javadoc:aggregate
    ```
 1. The javadocs are generated and stored in `target/site/apidocs`.

## Viewing In IntelliJ

 1. Go to `target/site/apidocs/index.html` in IntelliJ's project view.
 2. Right click on `target/site/apidocs/index.html`
 3. Select **Open in Browser**.
