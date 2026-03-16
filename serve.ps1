$listener = New-Object System.Net.HttpListener
$listener.Prefixes.Add("http://localhost:8080/")
$listener.Prefixes.Add("http://192.168.1.107:8080/")
$listener.Start()
Write-Host "Server running at http://localhost:8080/"
Write-Host "Mobile/Network Access: http://192.168.1.107:8080/"

while ($listener.IsListening) {
    $ctx = $listener.GetContext()
    $url = $ctx.Request.Url.LocalPath
    if ($url -eq "/") { $url = "/index.html" }
    $basePath = "C:\Users\Admin\.gemini\antigravity\scratch\distributor-app"
    $filePath = Join-Path $basePath ($url.TrimStart("/"))
    
    if (Test-Path $filePath) {
        $bytes = [System.IO.File]::ReadAllBytes($filePath)
        $ext = [System.IO.Path]::GetExtension($filePath)
        $contentType = switch ($ext) {
            ".html" { "text/html; charset=utf-8" }
            ".css"  { "text/css; charset=utf-8" }
            ".js"   { "application/javascript; charset=utf-8" }
            default { "application/octet-stream" }
        }
        $ctx.Response.ContentType = $contentType
        $ctx.Response.OutputStream.Write($bytes, 0, $bytes.Length)
    } else {
        $ctx.Response.StatusCode = 404
    }
    $ctx.Response.Close()
}
