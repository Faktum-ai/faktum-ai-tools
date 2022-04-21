import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="faktum-ai-tools",
    version="0.1.6",
    author="faktum-ai",
    author_email="henrik@planmate.ai",
    description="Faktum AI Util",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Faktum-ai/faktum-ai-tools/",
    project_urls={
        "Bug Tracker": "https://github.com/Faktum-ai/faktum-ai-tools/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.7",
    install_requires=[
          'pandas>=1.3',
      ],
)
