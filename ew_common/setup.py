import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ew_common",
    # This package is not versioned because other projects in the monorepo
    # always import the latest code from the local file system, i.e. HEAD of the
    # current git branch.
    version="HEAD",
    description="Shared code across EW Python codebase",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://www.elizabethwarren.com",
    packages=setuptools.find_packages(),
    install_requires=[
        "boto3==1.9.177",
        "contentful==1.12.3",
        "googlemaps==3.1.3",
        "nameparser==1.0.4",
        "phonenumberslite==8.10.22",
        "xmltodict==0.12.0",
    ],
    python_requires=">=3.7",
)
