import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fh:
   requirements = fh.readlines()

setuptools.setup(
    name="openalexnet",
    version="0.1.2",
    author="Filipi N. Silva",
    author_email="filipinascimento@gmail.com",
    description="Python library to load get networks from the OpenAlex API",
    install_requires=[req for req in requirements if req[:2] != "# "],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/filipinascimento/openalexnet",
    entry_points={
        'console_scripts': [
            'openalexnet = openalexnet.__main__:main',
        ],
    },
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)