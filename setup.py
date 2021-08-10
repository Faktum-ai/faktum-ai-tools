import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="faktum_ai_tools",
    version="0.0.2",
    author="faktum-ai",
    author_email="henrik@dataops.dk",
    description="Faktum AI Util",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Faktum-ai/faktum_ai_tools/",
    project_urls={
        "Bug Tracker": "https://github.com/Faktum-ai/faktum_ai_tools/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.8",
    
)