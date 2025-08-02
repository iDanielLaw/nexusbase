# 1. เข้าไปยังโฟลเดอร์ที่มีไฟล์ .proto
mkdir -p nexus

# 2. รันคำสั่ง generate code โดยระบุ output path ให้ชี้กลับไปยัง clients/python
#    (../../../clients/python)
python3 -m grpc_tools.protoc \
    -I../../api/tsdb \
    --python_out=./nexus \
    --pyi_out=./nexus \
    --grpc_python_out=./nexus \
    tsdb.proto

# สร้างไฟล์ __init__.py เพื่อให้ Python รู้จัก 'nexus' เป็น package
touch ./nexus/__init__.py

# แก้ไข import ในไฟล์ที่ generate ขึ้นมาให้เป็นแบบ relative
# This is a common issue when generating code into a subdirectory package.
sed -i 's/^import tsdb_pb2 as tsdb__pb2/from . import tsdb_pb2 as tsdb__pb2/' ./nexus/tsdb_pb2_grpc.py


echo "Generate ไฟล์สำเร็จ! ไฟล์ tsdb_pb2.py และ tsdb_pb2_grpc.py ถูกสร้างขึ้นใน clients/python/"
