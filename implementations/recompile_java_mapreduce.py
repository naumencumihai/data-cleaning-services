import os
import sys
import subprocess
import re

def is_camel_case(name):
    return bool(re.match(r'^[a-z]+([A-Z][a-z0-9]*)*$', name) or re.match(r'^[A-Z][a-z0-9]*([A-Z][a-z0-9]*)*$', name))

def camel_to_snake(name):
    s1 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return s1.lower()

def recompile_java_mapreduce(implementation_name):
    if not is_camel_case(implementation_name):
        print(f"Error: '{implementation_name}' is not in camelCase or PascalCase format.")
        sys.exit(1)

    try:
        # Convert to snake_case for directory name
        directory_name = camel_to_snake(implementation_name)

        if not os.path.exists(directory_name):
            os.makedirs(directory_name)

        hadoop_classpath = subprocess.check_output("hadoop classpath", shell=True).decode().strip()
        
        # Compile the Java files
        javac_command = f'javac -classpath {hadoop_classpath} -d ./{directory_name}/ ./{directory_name}/{implementation_name}Mapper.java ./{directory_name}/{implementation_name}Reducer.java ./{directory_name}/{implementation_name}Driver.java'
        subprocess.run(javac_command, shell=True)

        # Create the JAR file
        jar_command = f"jar cvf ./{directory_name}/{implementation_name}.jar -C {directory_name} ."
        subprocess.run(jar_command, shell=True)

        print(f"Successfully compiled and packaged {directory_name}/{implementation_name}.jar")

    except subprocess.CalledProcessError as e:
        print(f"Error during compilation or packaging: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python recompile_java_mapreducer_implementation.py [IMPLEMENTATION_NAME]")
        sys.exit(1)

    implementation_name = sys.argv[1]
    recompile_java_mapreduce(implementation_name)
