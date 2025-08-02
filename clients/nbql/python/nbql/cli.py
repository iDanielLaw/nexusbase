import argparse
import sys
import json
from nbql import Client, APIError, ConnectionError

def main():
    """
    Main function for the NBQL CLI.
    """
    parser = argparse.ArgumentParser(
        description="A command-line interface for Nexusbase (NBQL)."
    )
    parser.add_argument(
        "query_and_params",
        nargs="*",
        metavar="QUERY_OR_PARAM",
        help="The NBQL query string, optionally followed by parameters for safe substitution. "
             "Example: \"QUERY ? TAGGED (host=?)\" system.cpu.usage server-01"
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="The database host (default: 127.0.0.1)."
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9925,
        help="The database port (default: 9925)."
    )

    args = parser.parse_args()

    try:
        client = Client(host=args.host, port=args.port)
        if args.query_and_params:
            # Single query mode
            query_template = args.query_and_params[0]
            params_str = args.query_and_params[1:]

            # Attempt to convert parameters to numeric types for correct quoting
            params = []
            for p_str in params_str:
                try:
                    params.append(int(p_str))
                except ValueError:
                    try:
                        params.append(float(p_str))
                    except ValueError:
                        params.append(p_str)

            result = client.query(query_template, *params)
            print(json.dumps(result, indent=2))
        else:
            # Interactive mode
            print(f"Connected to nbql at {args.host}:{args.port}. Type 'exit' to quit.")
            print("NOTE: Interactive mode does not support query parameters. Please provide the full query string.")
            while True:
                try:
                    line = input("nbql> ")
                    if line.strip().lower() in ('exit', 'quit'):
                        break
                    if not line.strip():
                        continue
                    # In interactive mode, we send the raw query without parameters
                    result = client.query(line)
                    print(json.dumps(result, indent=2))
                except (APIError, ConnectionError) as e:
                    print(f"Error: {e}", file=sys.stderr)
                except (EOFError, KeyboardInterrupt):
                    break
            print("\nBye!")

    except (APIError, ConnectionError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    main()