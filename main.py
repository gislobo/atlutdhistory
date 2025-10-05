import subprocess
import sys


def main():
    # List of scripts to call
    scripts = [
        "players.py",
        "fixture.py",
        "event.py",
        "statistics.py",
        "playerstatistics.py",
        "lineups.py"
    ]

    print("You will be prompted for the fixture ID by each script.")

    # Call each script with inherited stdin
    for script in scripts:
        print(f"\n{'=' * 50}")
        print(f"Running {script}...")
        print(f"{'=' * 50}\n")

        # Run the script, inheriting stdin so it can prompt the user
        return_code = subprocess.call([sys.executable, script])

        # Check if script failed
        if return_code != 0:
            print(f"\n⚠️  Warning: {script} exited with code {return_code}")
            break
        else:
            print(f"\n✓ {script} completed successfully")


if __name__ == "__main__":
    main()