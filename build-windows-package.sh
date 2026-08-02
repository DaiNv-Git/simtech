#!/bin/bash
# ============================================
# Build Windows Distribution Package
# Creates a self-contained ZIP with JDK + JavaFX + EXE
# that can run on any Windows machine without pre-installed Java
# ============================================

set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
VERSION="1.0.0"
PACKAGE_NAME="simtech-${VERSION}-windows-${TIMESTAMP}"
BUILD_DIR="${PROJECT_DIR}/dist/build-${TIMESTAMP}"
OUTPUT_DIR="${PROJECT_DIR}/dist"
JDK_ZIP="${PROJECT_DIR}/openjdk-17-windows.zip"

echo "============================================"
echo "  simTech - Windows Package Builder"
echo "============================================"
echo ""
echo "Project: ${PROJECT_DIR}"
echo "Package: ${PACKAGE_NAME}"
echo ""

# --- Step 1: Check prerequisites ---
echo "[1/5] Checking prerequisites..."
if [ ! -f "${JDK_ZIP}" ]; then
    echo "[ERROR] Windows JDK ZIP not found at: ${JDK_ZIP}"
    echo "Please download OpenJDK 17 for Windows and place it as 'openjdk-17-windows.zip' in the project root."
    exit 1
fi
echo "  -> Found JDK ZIP: openjdk-17-windows.zip"

# --- Step 2: Build JAR + EXE + JavaFX libs ---
echo "[2/5] Building application JAR + EXE + JavaFX libs..."
cd "${PROJECT_DIR}"
./mvnw clean package -DskipTests -q

echo "  -> Cleaning up non-Windows JavaFX natives from module path..."
find target/javafx-lib -name "*-mac.jar" -delete 2>/dev/null || true
find target/javafx-lib -name "*-mac-aarch64.jar" -delete 2>/dev/null || true
find target/javafx-lib -name "*-linux.jar" -delete 2>/dev/null || true

JAR_FILE=$(ls target/simtech-*.jar 2>/dev/null | grep -v original | head -1)
EXE_FILE="target/simTech.exe"
JAVAFX_LIB="target/javafx-lib"

if [ -z "$JAR_FILE" ]; then
    echo "[ERROR] Build failed - no JAR file found in target/"
    exit 1
fi
echo "  -> JAR built: ${JAR_FILE}"

if [ ! -f "$EXE_FILE" ]; then
    echo "[WARN] EXE not generated - will use start.bat instead"
else
    echo "  -> EXE built: ${EXE_FILE}"
fi

if [ ! -d "$JAVAFX_LIB" ]; then
    echo "[ERROR] JavaFX libs not found in target/javafx-lib/"
    exit 1
fi
echo "  -> JavaFX Windows libs extracted"

# --- Step 3: Create distribution folder ---
echo "[3/5] Creating distribution folder..."
mkdir -p "${BUILD_DIR}/${PACKAGE_NAME}"
DIST_ROOT="${BUILD_DIR}/${PACKAGE_NAME}"

# --- Step 4: Copy files ---
echo "[4/5] Assembling package..."

# Copy EXE
if [ -f "$EXE_FILE" ]; then
    cp "${EXE_FILE}" "${DIST_ROOT}/simTech.exe"
    echo "  -> Copied simTech.exe"
fi

# Copy JAR
cp "${JAR_FILE}" "${DIST_ROOT}/simtech.jar"
echo "  -> Copied JAR"

# Copy JavaFX libs
cp -r "${JAVAFX_LIB}" "${DIST_ROOT}/javafx-lib"
echo "  -> Copied JavaFX Windows native libs"

# Extract Windows JDK from ZIP into the package
echo "  -> Extracting Windows JDK (this may take a moment)..."
TEMP_JDK_DIR="${BUILD_DIR}/jdk-extract"
mkdir -p "${TEMP_JDK_DIR}"
unzip -q "${JDK_ZIP}" -d "${TEMP_JDK_DIR}"

JDK_EXTRACTED=$(ls -d "${TEMP_JDK_DIR}"/jdk-* 2>/dev/null | head -1)
if [ -z "${JDK_EXTRACTED}" ]; then
    echo "[ERROR] Could not find extracted JDK folder"
    exit 1
fi

mv "${JDK_EXTRACTED}" "${DIST_ROOT}/jdk"
rm -rf "${TEMP_JDK_DIR}"
echo "  -> JDK extracted and bundled"

# Copy start scripts
cp "${PROJECT_DIR}/start.bat" "${DIST_ROOT}/"
cp "${PROJECT_DIR}/Tao_Icon_Desktop.bat" "${DIST_ROOT}/"
cp "${PROJECT_DIR}/HUONG_DAN_CAI_DAT.txt" "${DIST_ROOT}/"
echo "  -> Copied start scripts, shortcut script and instructions"

# Create empty directories
mkdir -p "${DIST_ROOT}/logs"
mkdir -p "${DIST_ROOT}/data"
echo "  -> Created logs/ and data/ directories"

# Copy application config if exists
if [ -d "${PROJECT_DIR}/src/main/resources" ]; then
    for cfg in application.yml application.properties application-prod.yml application-prod.properties; do
        if [ -f "${PROJECT_DIR}/src/main/resources/${cfg}" ]; then
            cp "${PROJECT_DIR}/src/main/resources/${cfg}" "${DIST_ROOT}/"
            echo "  -> Copied ${cfg} (for reference/override)"
        fi
    done
fi

# --- Step 5: Create ZIP ---
echo "[5/5] Creating ZIP archive..."
cd "${BUILD_DIR}"
zip -r "${OUTPUT_DIR}/${PACKAGE_NAME}.zip" "${PACKAGE_NAME}" -q
echo "  -> Created: dist/${PACKAGE_NAME}.zip"

# Cleanup build directory
rm -rf "${BUILD_DIR}"

# Show package info
ZIP_SIZE=$(du -h "${OUTPUT_DIR}/${PACKAGE_NAME}.zip" | cut -f1)
echo ""
echo "============================================"
echo "  BUILD COMPLETE!"
echo "============================================"
echo ""
echo "  Package: dist/${PACKAGE_NAME}.zip"
echo "  Size:    ${ZIP_SIZE}"
echo ""
echo "  To deploy on Windows:"
echo "  1. Copy the ZIP to the Windows machine"
echo "  2. Extract the ZIP"
echo "  3. Double-click 'simTech.exe' to run"
echo "     (or 'start.bat' as backup)"
echo "  4. Optionally run 'Tao_Icon_Desktop.bat' to create a Home Screen shortcut."
echo ""
echo "  No Java installation required!"
echo "============================================"
