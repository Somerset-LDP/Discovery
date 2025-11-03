param(
    [Parameter(Mandatory=$true)]
    [string]$InputFolder,
    
    [Parameter(Mandatory=$true)]
    [string]$OutputFolder,
    
    [Parameter(Mandatory=$true)]
    [string]$ConfigFile,

    [Parameter(Mandatory=$true)]
    [string]$SecurityToken,

    [Parameter(Mandatory=$true)]
    [string]$Credential,

    [Parameter(Mandatory=$true)]
    [string]$EncryptionKey,
    
    [Parameter(Mandatory=$false)]
    [switch]$DryRun
)

# Remove PowerShell's curl alias to use actual curl.exe
if (Get-Alias curl -ErrorAction SilentlyContinue) {
    Remove-Item alias:curl
}

# Function to write colored output
function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = "White"
    )
    Write-Host $Message -ForegroundColor $Color
}

# Function to generate SHA-256 hash
function Get-SHA256Hash {
    param([string]$FilePath)
    
    try {
        $hash = Get-FileHash -Path $FilePath -Algorithm SHA256
        return $hash.Hash
    }
    catch {
        Write-ColorOutput "Error generating hash for $FilePath`: $_" "Red"
        return $null
    }
}

# Function to upload file using curl
function Upload-FileWithCurl {
    param(
        [string]$FilePath,
        [string]$FileName,
        [string]$Date,
        [string]$Signature,
        [bool]$DryRun = $false
    )
    
    $url = "https://somerset-647582858282-dev-cohort-upload.s3.amazonaws.com/uploads/registered-patient/files/$FileName" +
           "?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
           "&X-Amz-Credential=$Credential" +
           "&X-Amz-Date=$Date" +
           "&X-Amz-Expires=3600" +
           "&X-Amz-SignedHeaders=host%3Bx-amz-server-side-encryption%3Bx-amz-server-side-encryption-aws-kms-key-id" +
           "&X-Amz-Security-Token=$SecurityToken" +
	   "&X-Amz-Signature=$Signature"
    
    # Build the curl command for display
    $curlCommand = "curl --location --request PUT `"$url`" " +
                   "--header `"X-Amz-Server-Side-Encryption: aws:kms`" " +
                   "--header `"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id: $EncryptionKey`" " +
                   "--header `"Content-Type: text/csv`" " +
                   "--data-binary `"@$FilePath`" " +
                   "--silent --show-error --write-out `"%{http_code}`""
    
    if ($DryRun) {
        Write-ColorOutput "  [DRY RUN] Would execute curl command:" "Magenta"
        Write-ColorOutput "  $curlCommand" "Yellow"
        return $true
    }
    
    $curlArgs = @(
        "--location",
        "--request", "PUT",
        $url,
        "--header", "X-Amz-Server-Side-Encryption: aws:kms",
        "--header", "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id: $EncryptionKey",
        "--header", "Content-Type: text/csv",
        "--data-binary", "@$FilePath",
        "--silent",
        "--show-error",
        "--write-out", "%{http_code}"
    )

   try {
        $result = & curl @curlArgs 2>&1
        
        # Parse HTTP code more robustly
        $httpCode = if ($result -is [array]) {
            ($result | Where-Object { $_ -match '^\d{3}$' } | Select-Object -Last 1)
        } else {
            $result.ToString().Trim()
        }
        
        # If we didn't find a clean 3-digit code, try to extract it
        if (-not $httpCode -or $httpCode -notmatch '^\d{3}$') {
            $fullOutput = $result -join ' '
            if ($fullOutput -match '\b(\d{3})\b') {
                $httpCode = $matches[1]
            }
        }
        
        if ($httpCode -eq "200" -or $httpCode -eq "204") {
            return $true
        } else {
            Write-ColorOutput "Upload failed with HTTP code: $httpCode" "Red"
            Write-ColorOutput "Full curl output: $($result -join ' ')" "Red"
            return $false
        }
    }
    catch {
        Write-ColorOutput "Error executing curl: $_" "Red"
        return $false
    }
}

# Main script execution
try {
    if ($DryRun) {
        Write-ColorOutput "=== DRY RUN MODE - NO FILES WILL BE UPLOADED ===" "Magenta"
    }
    
    Write-ColorOutput "Starting cohort file upload process..." "Green"
    Write-ColorOutput "Input folder: $InputFolder" "Cyan"
    Write-ColorOutput "Output folder: $OutputFolder" "Cyan"
    Write-ColorOutput "Config file: $ConfigFile" "Cyan"
    Write-ColorOutput "Dry run mode: $DryRun" "Cyan"
    
    if (-not (Test-Path $InputFolder)) {
        throw "Input folder does not exist: $InputFolder"
    }
    
    if (-not (Test-Path $ConfigFile)) {
        throw "Configuration file does not exist: $ConfigFile"
    }
    
    if (-not (Test-Path $OutputFolder)) {
        if ($DryRun) {
            Write-ColorOutput "[DRY RUN] Would create output folder: $OutputFolder" "Magenta"
        } else {
            New-Item -ItemType Directory -Path $OutputFolder -Force | Out-Null
            Write-ColorOutput "Created output folder: $OutputFolder" "Yellow"
        }
    }
    
    try {
        & curl --version | Out-Null
    }
    catch {
        throw "curl is not available. Please install curl or ensure it's in your PATH."
    }
    
    Write-ColorOutput "Loading configuration..." "Yellow"
    $configContent = Get-Content $ConfigFile -Raw
    $config = $configContent | ConvertFrom-Json
    
    $configHash = @{}
    $config.PSObject.Properties | ForEach-Object {
        $configHash[$_.Name] = $_.Value
    }
    
    Write-ColorOutput "Configuration loaded. Found $($configHash.Count) ODS entries." "Yellow"
    
    $cohortFiles = Get-ChildItem -Path $InputFolder -Filter "cohort_*.csv"
    Write-ColorOutput "Found $($cohortFiles.Count) cohort files to process." "Yellow"
    
    if ($cohortFiles.Count -eq 0) {
        Write-ColorOutput "No cohort files found in input folder." "Yellow"
        exit 0
    }
    
    $successCount = 0
    $failureCount = 0
    $missingConfigCount = 0
    
    foreach ($file in $cohortFiles) {
        Write-ColorOutput "`nProcessing: $($file.Name)" "White"
        
        if ($file.Name -match '^cohort_(.+)\.csv$') {
            $ods = $matches[1]
            Write-ColorOutput "  ODS: $ods" "Gray"
            
            if ($configHash.ContainsKey($ods)) {
                $odsConfig = $configHash[$ods]
                $date = $odsConfig[0]
                $signature = $odsConfig[1]
                
                Write-ColorOutput "  Found configuration for ODS: $ods" "Gray"
                Write-ColorOutput "  Date: $date" "Gray"
                Write-ColorOutput "  Signature: $($signature.Substring(0, [Math]::Min(20, $signature.Length)))..." "Gray"
                
                # Generate SHA-256 hash (always do this, even in dry run)
                Write-ColorOutput "  Generating SHA-256 hash..." "Gray"
                $hash = Get-SHA256Hash -FilePath $file.FullName
                
                if ($hash) {
                    $hashFileName = $file.Name -replace '\.csv$', '.sha256'
                    $hashFilePath = Join-Path $OutputFolder $hashFileName
                    
                    # Always save the hash file (even in dry run)
                    try {
                        $hash | Out-File -FilePath $hashFilePath -Encoding ASCII
                        Write-ColorOutput "  Hash saved to: $hashFileName" "Green"
                    }
                    catch {
                        Write-ColorOutput "  Error saving hash file: $_" "Red"
                        $failureCount++
                        continue
                    }
                    
                    # Only skip the upload in dry run mode
                    Write-ColorOutput "  Uploading file..." "Gray"
                    $uploadSuccess = Upload-FileWithCurl -FilePath $file.FullName -FileName $file.Name -Date $date -Signature $signature -DryRun $DryRun
                    
                    if ($uploadSuccess) {
                        if ($DryRun) {
                            Write-ColorOutput "  ✓ [DRY RUN] Upload command prepared successfully" "Green"
                        } else {
                            Write-ColorOutput "  ✓ Upload successful" "Green"
                        }
                        $successCount++
                    } else {
                        Write-ColorOutput "  ✗ Upload failed" "Red"
                        $failureCount++
                    }
                } else {
                    Write-ColorOutput "  ✗ Failed to generate hash" "Red"
                    $failureCount++
                }
            } else {
                Write-ColorOutput "  ✗ No configuration found for ODS: $ods" "Red"
                $missingConfigCount++
            }
        } else {
            Write-ColorOutput "  ✗ Invalid filename format: $($file.Name)" "Red"
            $failureCount++
        }
    }
    
    Write-ColorOutput "`n=== SUMMARY ===" "Cyan"
    if ($DryRun) {
        Write-ColorOutput "DRY RUN MODE - No actual uploads performed" "Magenta"
    }
    Write-ColorOutput "Total files processed: $($cohortFiles.Count)" "White"
    Write-ColorOutput "Successful uploads: $successCount" "Green"
    Write-ColorOutput "Failed uploads: $failureCount" "Red"
    Write-ColorOutput "Missing configuration: $missingConfigCount" "Yellow"
    
    if ($failureCount -gt 0 -or $missingConfigCount -gt 0) {
        Write-ColorOutput "`nProcess completed with errors." "Red"
        exit 1
    } else {
        if ($DryRun) {
            Write-ColorOutput "`nDry run completed successfully." "Green"
        } else {
            Write-ColorOutput "`nProcess completed successfully." "Green"
        }
        exit 0
    }
}
catch {
    Write-ColorOutput "Fatal error: $_" "Red"
    exit 1
}
