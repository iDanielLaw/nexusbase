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
        "query",
        nargs="?",
        default=None,
        help="The NBQL query to execute. If not provided, runs in interactive mode."
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="The database host (default: 127.0.0.1)."
    )
    parser.add_argument(
        "--port",
        type=int,
        default=50052,
        help="The database port (default: 50052)."
    )
    parser.add_argument(
        "--file",
        help="Path to a JSON file containing an array of data points to push in bulk."
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help="Number of points to send in each bulk push chunk. Helps manage server memory."
    )

    args = parser.parse_args()

    if args.query and args.file:
        print("Error: 'query' and '--file' arguments are mutually exclusive.", file=sys.stderr)
        sys.exit(1)

    try:
        client = Client(host=args.host, port=args.port)

        if args.file:
            # Bulk push from file mode
            try:
                with open(args.file, 'r', encoding='utf-8') as f:
                    points = json.load(f)
                if not isinstance(points, list):
                    raise TypeError("JSON file must contain a list (array) of data points.")
                result = client.push_bulk(points, chunk_size=args.chunk_size)
                print(json.dumps(result, indent=2))
            except FileNotFoundError:
                print(f"Error: File not found at '{args.file}'", file=sys.stderr)
                sys.exit(1)
            except json.JSONDecodeError:
                print(f"Error: Could not decode JSON from '{args.file}'. Please check the file format.", file=sys.stderr)
                sys.exit(1)
            except (TypeError, ValueError) as e:
                print(f"Error processing file content: {e}", file=sys.stderr)
                sys.exit(1)
        elif args.query:
            # Single query mode
            result = client.query(args.query)
            print(json.dumps(result, indent=2))
        else:
            # Interactive mode
            print(f"Connected to nbql at {args.host}:{args.port}. Type 'exit' to quit.")
            while True:
                try:
                    line = input("nbql> ")
                    if line.strip().lower() in ('exit', 'quit'):
                        break
                    if not line.strip():
                        continue
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
