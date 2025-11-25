import subprocess
import sys
from pathlib import Path

proto_dir = Path("proto").absolute()
target_dir = Path("src/nebulagraph_python/proto").absolute()


def generate_proto_files():
    try:
        import grpc_tools.protoc  # noqa
    except ImportError:
        print("Installing required dependencies...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "grpcio-tools"])

    # Get the directory containing the .proto files

    # Proto files to generate
    proto_files = [
        "nebula_common.proto",
        "vector.proto",
        "graph.proto",
    ]  # Order matters! Dependencies first

    for proto_file in proto_files:
        proto_path = proto_dir / proto_file
        if not proto_path.exists():
            print(f"Error: Proto file not found: {proto_path}")
            continue

        print(f"Generating Python files for {proto_file}...")

        # Command to generate Python code from proto file
        cmd = [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"--proto_path={proto_dir}",
            f"--python_out={target_dir}",
            f"--grpc_python_out={target_dir}",
            f"--pyi_out={target_dir}",
            "--experimental_allow_proto3_optional",  # Add this flag
            str(proto_path),
        ]

        try:
            # Run the protoc compiler
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if result.returncode != 0:
                print(f"Error output: {result.stderr}")
                raise subprocess.CalledProcessError(
                    result.returncode, cmd, result.stdout, result.stderr
                )
            print(f"Successfully generated files for {proto_file}")

            # Fix imports in generated files
            file_name_base = proto_file.replace(".proto", "")
            for generated_file in [
                target_dir / f"{file_name_base}_pb2.py",
                target_dir / f"{file_name_base}_pb2_grpc.py",
                target_dir / f"{file_name_base}_pb2.pyi",
            ]:
                if generated_file.exists():
                    content = generated_file.read_text()
                    # Handle both the new nebula_common and backward compatibility
                    content = (
                        content.replace(
                            "import nebula_common_pb2",
                            "from . import nebula_common_pb2",
                        )
                        .replace(
                            "import common_pb2",
                            "from . import nebula_common_pb2 as common_pb2",
                        )
                        .replace(
                            "import vector_pb2",
                            "from . import vector_pb2",
                        )
                        .replace(
                            "import graph_pb2",
                            "from . import graph_pb2",
                        )
                    )

                    generated_file.write_text(content)

        except subprocess.CalledProcessError as e:
            print(f"Error generating files for {proto_file}:")
            print(f"Command output: {e.output}")
            print(f"Error output: {e.stderr}")
            raise


if __name__ == "__main__":
    try:
        generate_proto_files()
    except Exception as e:
        print(f"Error: {e!s}")
        sys.exit(1)
