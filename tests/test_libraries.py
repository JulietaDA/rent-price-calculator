import subprocess
import pytest
import os

def get_installed_packages():
    """Get a dictionary of installed packages and their versions."""
    result = subprocess.run(
        ['pip', 'freeze'], capture_output=True, text=True
    )
    installed_packages = {}
    for line in result.stdout.splitlines():
        package, version = line.split('==')
        installed_packages[package.lower()] = version
    return installed_packages

def get_requirements():
    """Get a dictionary of packages and their versions from requirements.txt."""
    req_path = os.path.join(os.path.dirname(__file__), '../requirements.txt')
    with open(req_path, 'r') as f:
        requirements = {}
        for line in f:
            # Ignore comments and empty lines
            line = line.strip()
            if line and not line.startswith('#'):
                package, version = line.split('==')
                requirements[package.lower()] = version
    return requirements

def test_requirements():
    """
    Test to ensure that installed packages match those specified in requirements.txt.
    """
    installed_packages = get_installed_packages()
    required_packages = get_requirements()

    for package, required_version in required_packages.items():
        installed_version = installed_packages.get(package)
        assert installed_version is not None, f"{package} is not installed."
        assert installed_version == required_version, (
            f"Version conflict for {package}: "
            f"installed version is {installed_version}, "
            f"but {required_version} is required."
        )

if __name__ == "__main__":
    pytest.main()
