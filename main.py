import subprocess


def main():
    # Define the argument you want to pass
    fixture_id = input("Enter the fixture ID: ")

    # List of scripts to call
    scripts = [
        "players.py",
        "fixture.py",
        "event.py",
        "statistics.py",
        "playerstatistics.py",
        "lineups.py"
    ]

    # Call each script, passing the fixture_id via stdin
    for script in scripts:
        print(f"\n{'=' * 50}")
        print(f"Running {script}...")
        print(f"{'=' * 50}\n")

        # Run the script with real-time output
        process = subprocess.Popen(
            ["python", script],
            stdin=subprocess.PIPE,
            text=True
        )

        # Send the fixture_id to the script's stdin and close it
        process.stdin.write(fixture_id + "\n")
        process.stdin.close()

        # Wait for the process to complete
        return_code = process.wait()

        # Check if script failed
        if return_code != 0:
            print(f"\n⚠️  Warning: {script} exited with code {return_code}")
            break  # Optional: stop if one script fails
        else:
            print(f"\n✓ {script} completed successfully")


if __name__ == "__main__":
    main()