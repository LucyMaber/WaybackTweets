from setuptools import setup, find_packages

setup(
    name="async_wayback_twitter",
    version="0.1.0",
    description="An asynchronous interface to fetch and parse archived Twitter data from the Wayback Machine.",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    install_requires=[
        "aiohttp",
        "rich",
    ],
    python_requires=">=3.7",
)
