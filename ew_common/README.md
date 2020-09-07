# EW Common Library

Shared code across codebase.

## Be careful around transitive dependencies!

Apps that use ew_common modules *must* explicitly add to the *app's*
Pipfile all dependencies of the ew_common modules that the app uses. So,
for example, if your app uses `ew_common/googlemaps`, you must add
`googlemaps` to your app's Pipfile; for another example, if your app
uses `ew_common/input_validation`, you must add `phonenumberslite` and
`nameparser` to your app's Pipfile.
