import shutil
from pathlib import Path

proto_dir = Path("proto").absolute()


def update_proto_references():
    # 1. Rename common.proto to nebula_common.proto
    old_file = proto_dir / "common.proto"
    new_file = proto_dir / "nebula_common.proto"

    if old_file.exists():
        shutil.move(old_file, new_file)
        print(f"Renamed {old_file} to {new_file}")

    # 2. Update references in other proto files
    proto_files = ["vector.proto", "graph.proto"]

    for proto_file in proto_files:
        file_path = proto_dir / proto_file
        if not file_path.exists():
            print(f"Warning: {proto_file} not found")
            continue

        # Read the content
        with open(file_path, "r") as f:
            content = f.read()

        # Replace the import statement
        updated_content = content.replace(
            'import "common.proto"', 'import "nebula_common.proto"'
        )

        # Write back the updated content
        with open(file_path, "w") as f:
            f.write(updated_content)
        print(f"Updated references in {proto_file}")


if __name__ == "__main__":
    update_proto_references()
