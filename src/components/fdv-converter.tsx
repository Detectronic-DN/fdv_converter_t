"use client";

import React, { useState, useCallback, useMemo, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { invoke } from "@tauri-apps/api/core";
import { open, save } from "@tauri-apps/plugin-dialog";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import {
  AlertCircle,
  FileSpreadsheet,
  Loader2,
  CalendarCog,
  X,
  FileText,
  CloudRain,
  ArrowRight,
  Calculator, Play, FolderPlus
} from "lucide-react";
import { listen } from "@tauri-apps/api/event";
import { path } from "@tauri-apps/api";
import { Spinner } from "@/components/spinner";

interface SiteDetails {
  siteId: string;
  siteName: string;
  startTimestamp: string;
  endTimestamp: string;
}

interface FileDetails {
  name: string;
  path: string;
}

interface BatchFileDetails extends FileDetails {
  pipeShape: string;
  pipeSize: string;
}

interface LogMessage {
  level: string;
  message: string;
}

interface ProcessedFileData {
  columnMapping: Record<
    string,
    Array<[string, number, string | null, string | null]>
  >;
  monitorType: string;
  startTimestamp: string;
  endTimestamp: string;
  interval: number;
  siteId: string;
  siteName: string;
}

export const FdvConverter: React.FC = () => {
  const [isLoading, setIsLoading] = useState(false)
  const [siteDetails, setSiteDetails] = useState<SiteDetails>({
    siteId: "",
    siteName: "",
    startTimestamp: "",
    endTimestamp: "",
  });
  const [username, setUsername] = useState("Guest");
  const [selectedFile, setSelectedFile] = useState<FileDetails | null>(null);
  const [isProcessing, setIsProcessing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [depthColumn, setDepthColumn] = useState<string>("");
  const [velocityColumn, setVelocityColumn] = useState<string>("");
  const [pipeShape, setPipeShape] = useState<string>("");
  const [pipeSize, setPipeSize] = useState<string>("");
  const [logs, setLogs] = useState<LogMessage[]>([]);
  const [processedData, setProcessedData] = useState<ProcessedFileData | null>(
    null
  );
  const [rainfallColumn, setRainfallColumn] = useState<string>("")
  const [eggType, setEggType] = useState("Egg Type 1");
  const [pipeWidth, setPipeWidth] = useState("");
  const [pipeHeight, setPipeHeight] = useState("");
  const [r3Value, setR3Value] = useState("");
  const [batchFiles, setBatchFiles] = useState<BatchFileDetails[]>([]);
  const [batchProcessing, setBatchProcessing] = useState(false);


  const allColumns = processedData?.columnMapping ? Object.values(processedData.columnMapping).flat() : [];

  const resetState = useCallback(() => {
    setSiteDetails({
      siteId: "",
      siteName: "",
      startTimestamp: "",
      endTimestamp: "",
    });
    setSelectedFile(null);
    setError(null);
    setDepthColumn("");
    setVelocityColumn("");
    setPipeShape("");
    setPipeSize("");
    setProcessedData(null);
    setLogs([]);
  }, []);

  const handleFileChange = useCallback(async () => {
    setIsLoading(true);
    try {
      const selected = await open({
        multiple: false,
        filters: [
          {
            name: "Spreadsheet",
            extensions: ["xlsx", "xls", "csv"],
          },
        ],
      });

      if (selected === null) {
        return;
      }

      resetState();

      try {
        await invoke("clear_command_handler_state");
        console.log("FileProcessor state reset successfully");
      } catch (error) {
        console.error("Failed to reset FileProcessor state:", error);
      }

      const fileName =
          selected.split("\\").pop() || selected.split("/").pop() || selected;
      setSelectedFile({ name: fileName, path: selected });
      setError(null);

      // Automatically process the file after selection
      await handleProcessFile();
    } catch (error) {
      setError(
          `Error selecting file: ${error instanceof Error ? error.message : String(error)}`
      );
      setSelectedFile(null);
    } finally {
      setIsLoading(false);
    }
  }, [resetState]);

  const handleProcessFile = useCallback(async () => {
    if (!selectedFile) {
      setError("No file selected. Please select a file first.");
      return;
    }

    try {
      setIsProcessing(true);
      setIsLoading(true);
      setError(null);

      const result = await invoke("process_file", {
        filePath: selectedFile.path,
      });
      const processedData = JSON.parse(result as string);

      setProcessedData(processedData);
      setSiteDetails((prev) => ({
        ...prev,
        siteId: processedData.siteId,
        siteName: processedData.siteName,
        startTimestamp: processedData.startTimestamp,
        endTimestamp: processedData.endTimestamp,
      }));
    } catch (error) {
      setError(
          `Error processing file: ${error instanceof Error ? error.message : String(error)}`
      );
    } finally {
      setIsProcessing(false);
      setIsLoading(false);
    }
  }, [selectedFile]);

  const handleUpdateSiteId = useCallback(async () => {
    setIsLoading(true);
    try {
      const result = await invoke<string>("update_site_id", {
        siteId: siteDetails.siteId,
      });
      const updatedInfo = JSON.parse(result);

      setSiteDetails((prev) => ({
        ...prev,
        siteId: updatedInfo.siteId,
      }));
    } catch (error) {
      setError(
        `Error updating site ID: ${error instanceof Error ? error.message : String(error)
        }`
      );
    }
    finally {
      setIsLoading(false);
    }
  }, [siteDetails.siteId]);

  const handleUpdateSiteName = useCallback(async () => {
    setIsLoading(true);
    try {
      const result = await invoke<string>("update_site_name", {
        siteName: siteDetails.siteName,
      });
      const updatedInfo = JSON.parse(result);

      setSiteDetails((prev) => ({
        ...prev,
        siteName: updatedInfo.siteName,
      }));

    } catch (error) {
      setError(
        `Error updating site name: ${error instanceof Error ? error.message : String(error)
        }`
      );
    }
    finally {
      setIsLoading(false);
    }
  }, [siteDetails.siteName]);

  const handleUpdateTimestamps = useCallback(async () => {
    try {
      setIsProcessing(true);
      setIsLoading(true);
      setError(null);

      const result = await invoke<string>("update_timestamps", {
        startTime: siteDetails.startTimestamp,
        endTime: siteDetails.endTimestamp,
      });
      const updatedData = JSON.parse(result);

      setSiteDetails((prev) => ({
        ...prev,
        startTimestamp: updatedData.startTimestamp,
        endTimestamp: updatedData.endTimestamp,
      }));

      setProcessedData((prev) =>
        prev
          ? {
            ...prev,
            startTimestamp: updatedData.startTimestamp,
            endTimestamp: updatedData.endTimestamp,
          }
          : null
      );
    } catch (error) {
      setError(
        `Error updating timestamps: ${error instanceof Error ? error.message : String(error)
        }`
      );
    } finally {
      setIsProcessing(false);
      setIsLoading(false);
    }
  }, [siteDetails.startTimestamp, siteDetails.endTimestamp]);

  const addLog = useCallback((level: string, message: string) => {
    setLogs((prevLogs) => [...prevLogs, { level, message }]);
  }, []);

  useEffect(() => {
    // Get recent logs
    invoke<LogMessage[]>("get_recent_logs")
      .then(setLogs)
      .catch((error) => console.error("Failed to get recent logs:", error));

    // Listen for new log messages
    const unlistenLogMessage = listen<LogMessage>("log_message", (event) => {
      addLog(event.payload.level, event.payload.message);
    });

    return () => {
      unlistenLogMessage.then((unlisten) => unlisten());
    };
  }, [addLog]);

  const handleKeyPress = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>, updateFunction: () => void) => {
      if (e.key === "Enter") {
        e.preventDefault();
        updateFunction();
      }
    },
    []
  );

  const getLogColor = (level: string) => {
    switch (level.toLowerCase()) {
      case "info":
        return "text-green-500";
      case "warn":
        return "text-yellow-500";
      case "error":
        return "text-red-500";
      default:
        return "text-gray-500";
    }
  };

  const handleUpdateSiteDetails = useCallback(
    (field: keyof SiteDetails, value: string) => {
      setSiteDetails((prev) => ({ ...prev, [field]: value }));
    },
    []
  );

  const handleUpdateUsername = useCallback((newUsername: string) => {
    setUsername(newUsername);
  }, []);
//todo:: set the username permanently



  const handleCreateFdv = async () => {
    setIsLoading(true);
    try {
      const suggestedFileName = `${processedData?.siteId || 'output'}.fdv`
      const savePath = await save({
        filters: [{
          name: 'FDV File',
          extensions: ['fdv']
        }],
        defaultPath: suggestedFileName
      })

      if (savePath) {
        const result = await invoke('create_fdv_flow', {
          outputPath: savePath,
          depthCol: depthColumn,
          velocityCol: velocityColumn === 'none' ? null : velocityColumn,
          pipeShape: pipeShape,
          pipeSize: pipeSize || ''
        })
        console.log("FDV created successfully:", result)
      }
    } catch (error) {
      console.error('Error creating FDV:', error)
    } finally {
      setIsLoading(false);
    }
  }

  const handleCreateRainfall = async () => {
    setIsLoading(true);
    try {
      const suggestedFileName = `${processedData?.siteId || 'output'}.r`
      const savePath = await save({
        filters: [{
          name: 'Rainfall File',
          extensions: ['r']
        }],
        defaultPath: suggestedFileName
      })

      if (savePath) {
        await invoke('create_rainfall', {
          outputPath: savePath,
          rainfallCol: rainfallColumn,
        })
      }
    } catch (error) {
      console.error('Error creating Rainfall Rainfall:', error)
    } finally {
      setIsLoading(false);
    }
  }

  const handleCalculateR3 = useCallback(async () => {
    if (!pipeWidth || !pipeHeight) {
      setError("Please fill in all required fields");
      return;
    }

    setError(null);

    try {
      const result = await invoke<string>("calculate_r3", {
        width: parseFloat(pipeWidth),
        height: parseFloat(pipeHeight),
        eggForm: eggType
      });

      const numericResult = parseFloat(result);

      if (isNaN(numericResult) || numericResult === -1) {
        setError("R3 calculation failed");
        return;
      }

      const formattedR3 = numericResult.toFixed(2);
      setR3Value(formattedR3);
    } catch (error) {
      const errorMessage = `Failed to calculate R3: ${(error as Error).message}`;
      setError(errorMessage);
    }
  }, [eggType, pipeWidth, pipeHeight]);

  const handleUseR3 = useCallback(() => {
    if (pipeWidth && pipeHeight && r3Value) {
      const newPipeSize = `${pipeWidth},${pipeHeight},${r3Value}`;
      setPipeSize(newPipeSize);
    } else {
      setError('Please calculate R3 value first');
    }
  }, [pipeWidth, pipeHeight, r3Value]);


  const handleInterimReports = async () => {
    setIsLoading(true);
    try {
      const suggestedFileName = `${processedData?.siteId || 'interim_reports'}.xlsx`;
      const savePath = await save({
        filters: [{
          name: 'Excel File',
          extensions: ['xlsx']
        }],
        defaultPath: suggestedFileName
      });

      if (savePath) {
        await invoke('generate_interim_reports', {
          outputPath: savePath
        });
        console.log("Interim reports saved successfully");
      }
    } catch (error) {
      console.error('Error saving interim reports:', error);
      setError(`Error saving interim reports: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      setIsLoading(false);
    }
  };

  const handleGenerateRainfallTotals = async () => {
    setIsLoading(true);
    try {
      const suggestedFileName = `${processedData?.siteId || 'rainfall_totals'}.xlsx`;
      const savePath = await save({
        filters: [{
          name: 'Excel File',
          extensions: ['xlsx']
        }],
        defaultPath: suggestedFileName
      });

      if (savePath) {
        await invoke('generate_rainfall_totals', {
          outputPath: savePath
        });
        console.log("Rainfall totals saved successfully");
      }
    } catch (error) {
      console.error('Error saving rainfall totals:', error);
      setError(`Error saving rainfall totals: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      setIsLoading(false);
    }
  };


  const handleBatchFileChange = useCallback(async () => {
    try {
      const selected = await open({
        multiple: true,
        filters: [
          {
            name: "Spreadsheet",
            extensions: ["xlsx", "xls", "csv"],
          },
        ],
      });

      if (Array.isArray(selected)) {
        const newFiles = selected.map((path) => {
          const fileName = path.split("\\").pop() || path.split("/").pop() || path;
          return { name: fileName, path, pipeShape: "Circular", pipeSize: "" };
        });
        setBatchFiles((prev) => [...prev, ...newFiles]);
      }
    } catch (error) {
      setError(
        `Error selecting files: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }, [setError]);

  const handleRemoveBatchFile = useCallback((index: number) => {
    setBatchFiles((prev) => prev.filter((_, i) => i !== index));
  }, []);

  const handleUpdateBatchFile = useCallback((index: number, field: 'pipeShape' | 'pipeSize', value: string) => {
    setBatchFiles((prev) => prev.map((file, i) =>
      i === index ? { ...file, [field]: value } : file
    ));
  }, []);

  const handleBatchProcess = useCallback(async () => {
    if (batchFiles.length === 0) {
      setError("No files selected for batch processing.");
      return;
    }
    
    try {
      setBatchProcessing(true);
      setIsLoading(true);
      setError(null);

      const outputDir = await open({
        directory: true,
        multiple: false,
        title: "Select Output Directory",
      });

      if (!outputDir) {
        setBatchProcessing(false);
        return;
      }

      const fileInfos = await Promise.all(batchFiles.map(async file => ({
        filepath: await path.normalize(file.path),
        pipeshape: file.pipeShape,
        pipesize: file.pipeSize
      })));

      await invoke<string>("run_batch_process", {
        fileInfos: fileInfos,
        outputDir: await path.normalize(outputDir)
      });
      setBatchFiles([]);
    } catch (error) {
      setError(
        `Error during batch processing: ${error instanceof Error ? error.message : String(error)}`
      );
    } finally {
      setBatchProcessing(false);
      setIsLoading(false);
    }
  }, [batchFiles]);

  const isFormValid = useMemo(() => {
    return (
      siteDetails.siteId.trim() !== "" &&
      siteDetails.siteName.trim() !== "" &&
      siteDetails.startTimestamp !== "" &&
      siteDetails.endTimestamp !== "" &&
      selectedFile !== null &&
      depthColumn !== "" &&
      pipeShape !== ""
    );
  }, [
    siteDetails,
    selectedFile,
    depthColumn,
    pipeShape,
  ]);

  return (
    <>
      <Spinner isLoading={isLoading} />
      <div className="container mx-auto p-4">
        <div className="flex justify-between items-center mb-4">
          <h1 className="text-2xl font-bold">FDV Converter</h1>
          <Sheet>
            <SheetTrigger asChild>
              <Button variant="ghost" className="flex items-center space-x-2">
                <span>{username}</span>
                <Avatar>
                  <AvatarFallback>{username[0]}</AvatarFallback>
                </Avatar>
              </Button>
            </SheetTrigger>
            <SheetContent>
              <SheetHeader>
                <SheetTitle>User Settings</SheetTitle>
                <SheetDescription>Update your username here.</SheetDescription>
              </SheetHeader>
              <div className="py-4">
                <Input
                  placeholder="Enter new username"
                  value={username}
                  onChange={(e) => handleUpdateUsername(e.target.value)}
                  aria-label="Username"
                />
              </div>
            </SheetContent>
          </Sheet>
        </div>

        <div className="flex mb-4">
          <Input
            type="text"
            placeholder="Choose File"
            className="mr-2"
            value={selectedFile ? selectedFile.name : ""}
            readOnly
            aria-label="Selected file"
          />
          <Button
              className="bg-[#2e7d32] hover:bg-[#1b5e20] text-white font-semibold py-2 px-4 rounded-t-md shadow-md transition-all duration-300 ease-in-out flex items-center space-x-2"
              onClick={handleFileChange}
              disabled={isProcessing}
          >
            <FileSpreadsheet className="w-5 h-5" />
            <span>Upload File</span>
          </Button>
        </div>

        {error && (
          <Alert variant="destructive" className="mb-4">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>Error</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        <Card className="mb-4">
          <CardHeader>
            <CardTitle>Site Details</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-4">
              <Input
                  placeholder="Site ID"
                  value={siteDetails.siteId}
                  onChange={(e) =>
                      setSiteDetails((prev) => ({...prev, siteId: e.target.value}))
                  }
                  onKeyDown={(e) => handleKeyPress(e, handleUpdateSiteId)}
                  aria-label="Site ID"
              />
              <Input
                  placeholder="Site Name"
                  value={siteDetails.siteName}
                  onChange={(e) =>
                      setSiteDetails((prev) => ({
                        ...prev,
                        siteName: e.target.value,
                      }))
                  }
                  onKeyDown={(e) => handleKeyPress(e, handleUpdateSiteName)}
                  aria-label="Site Name"
              />
              <Input
                  type="datetime-local"
                  placeholder="Start Timestamp"
                  value={siteDetails.startTimestamp}
                  onChange={(e) =>
                      handleUpdateSiteDetails("startTimestamp", e.target.value)
                  }
                  aria-label="Start Timestamp"
              />
              <Input
                  type="datetime-local"
                  placeholder="End Timestamp"
                  value={siteDetails.endTimestamp}
                  onChange={(e) =>
                      handleUpdateSiteDetails("endTimestamp", e.target.value)
                  }
                  aria-label="End Timestamp"
              />
            </div>
            <div className="flex justify-end mt-4">
              <Button
                  className="bg-[#f44336] hover:bg-[#d32f2f] text-white font-semibold py-2 px-4 rounded-full shadow-md transition-all duration-300 ease-in-out flex items-center space-x-2"
                  onClick={handleUpdateTimestamps}
                  disabled={isProcessing || !processedData}
              >
                <CalendarCog className="w-5 h-5"/>
                <span>
                  {isProcessing ? "Updating..." : "Update Timestamps"}
                </span>
              </Button>
            </div>
          </CardContent>
        </Card>

        <Tabs defaultValue="fdv-converter">
          <TabsList>
            <TabsTrigger value="fdv-converter">FDV Converter</TabsTrigger>
            <TabsTrigger value="rainfall">Rainfall</TabsTrigger>
            <TabsTrigger value="r3-calculator">R3 Calculator</TabsTrigger>
            <TabsTrigger value="batch-processing">Batch Processing</TabsTrigger>
          </TabsList>
          <TabsContent value="fdv-converter">
            <div className="grid grid-cols-2 gap-4 mt-4">
              <Select value={depthColumn} onValueChange={setDepthColumn}>
                <SelectTrigger>
                  <SelectValue placeholder="Select depth column" />
                </SelectTrigger>
                <SelectContent>
                  {allColumns.map(([columnName, index]) => (
                    <SelectItem key={`${columnName}-${index}`} value={columnName}>
                      {columnName}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={velocityColumn} onValueChange={setVelocityColumn}>
                <SelectTrigger>
                  <SelectValue placeholder="Select velocity column" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">None</SelectItem>
                  {allColumns.map(([columnName, index]) => (
                    <SelectItem key={`${columnName}-${index}`} value={columnName}>
                      {columnName}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={pipeShape} onValueChange={setPipeShape}>
                <SelectTrigger>
                  <SelectValue placeholder="Pipe Shape" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="Circular">Circular</SelectItem>
                  <SelectItem value="Rectangular">Rectangular</SelectItem>
                  <SelectItem value="Egg Type 1">Egg Type 1</SelectItem>
                  <SelectItem value="Egg Type 2">Egg Type 2</SelectItem>
                  <SelectItem value="Egg Type 2A">Egg Type 2A</SelectItem>
                  <SelectItem value="Two Circle and Rectangle">Two Circle and Rectangle</SelectItem>
                </SelectContent>
              </Select>
              <Input
                placeholder="Enter pipe size"
                value={pipeSize}
                onChange={(e) => setPipeSize(e.target.value)}
                aria-label="Pipe Size"
              />
              <Button
                  className="col-span-1 bg-gradient-to-r from-indigo-700 to-indigo-900 hover:from-indigo-800 hover:to-indigo-950 text-white font-semibold py-2 px-4 rounded-md shadow-md transition-all duration-300 ease-in-out flex items-center justify-center space-x-2"
                  onClick={handleInterimReports}
                  disabled={!processedData || isLoading}
              >
                <FileText className="w-5 h-5" />
                <span>Interim Reports</span>
              </Button>
              <Button
                  className="col-span-1 bg-gradient-to-r from-blue-400 to-blue-600 hover:from-blue-500 hover:to-blue-700 text-white font-semibold py-2 px-4 rounded-md shadow-md transition-all duration-300 ease-in-out"
                  disabled={!isFormValid}
                  onClick={handleCreateFdv}
              >
                Create FDV
              </Button>
            </div>
          </TabsContent>
          <TabsContent value="rainfall">
            <div className="grid grid-cols-1 gap-4 mt-4">
              <Select value={rainfallColumn} onValueChange={setRainfallColumn}>
                <SelectTrigger>
                  <SelectValue placeholder="Select rainfall column"/>
                </SelectTrigger>
                <SelectContent>
                  {allColumns.map(([columnName, index]) => (
                      <SelectItem key={`${columnName}-${index}`} value={columnName}>
                        {columnName}
                      </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <div className="flex justify-between space-x-4">
                <Button
                    className="flex-1 bg-gradient-to-r from-indigo-700 to-indigo-900 hover:from-indigo-800 hover:to-indigo-950 text-white font-semibold py-2 px-4 rounded-md shadow-md transition-all duration-300 ease-in-out flex items-center justify-center space-x-2"
                    onClick={handleGenerateRainfallTotals}
                    disabled={isLoading || !processedData || processedData.monitorType !== "Rainfall"}
                >
                  <FileText className="w-5 h-5"/>
                  <span>Rainfall Totals</span>
                </Button>
                <Button
                    className="flex-1 bg-gradient-to-r from-blue-400 to-blue-600 hover:from-blue-500 hover:to-blue-700 text-white font-semibold py-2 px-4 rounded-md shadow-md transition-all duration-300 ease-in-out flex items-center justify-center space-x-2"
                    onClick={handleCreateRainfall}
                    disabled={isProcessing || !rainfallColumn}
                >
                  <CloudRain className="w-5 h-5"/>
                  <span>Create Rainfall</span>
                </Button>
              </div>
            </div>
          </TabsContent>
          <TabsContent value="r3-calculator">
            <Card>
              <CardHeader>
                <CardTitle>R3 Calculator</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 gap-4">
                  <Select value={eggType} onValueChange={setEggType}>
                    <SelectTrigger>
                      <SelectValue placeholder="Egg Type"/>
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Egg Type 1">Egg Type 1</SelectItem>
                      <SelectItem value="Egg Type 2">Egg Type 2</SelectItem>
                    </SelectContent>
                  </Select>
                  <Input
                      type="number"
                      placeholder="Pipe Width (mm)"
                      value={pipeWidth}
                      onChange={(e) => setPipeWidth(e.target.value)}
                  />
                  <Input
                      type="number"
                      placeholder="Pipe Height (mm)"
                      value={pipeHeight}
                      onChange={(e) => setPipeHeight(e.target.value)}
                  />
                  <Input
                    type="text"
                    placeholder="R3 Value (mm)"
                    value={r3Value}
                    readOnly
                  />
                  <Button
                      onClick={handleCalculateR3}
                      className="col-span-1 bg-emerald-500 hover:bg-emerald-600 text-white font-semibold py-2 px-4 rounded-lg transition-colors duration-300 ease-in-out flex items-center justify-center space-x-2"
                      disabled={isLoading}
                  >
                    <Calculator className="w-5 h-5" />
                    <span>Calculate R3</span>
                  </Button>
                  <Button
                      onClick={handleUseR3}
                      className="col-span-1 bg-amber-500 hover:bg-amber-600 text-white font-semibold py-2 px-4 rounded-lg transition-colors duration-300 ease-in-out flex items-center justify-center space-x-2"
                      disabled={!r3Value}
                  >
                    <ArrowRight className="w-5 h-5" />
                    <span>Use R3 value</span>
                  </Button>
                </div>
                {error && (
                  <p className="text-red-500 mt-2">{error}</p>
                )}
              </CardContent>
            </Card>
          </TabsContent>
          <TabsContent value="batch-processing">
            <Card>
              <CardHeader>
                <CardTitle>Batch Processing</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div className="flex items-center space-x-2">
                    <Button
                        onClick={handleBatchFileChange}
                        disabled={batchProcessing}
                        className="bg-purple-600 hover:bg-purple-700 text-white font-semibold py-2 px-4 rounded-lg transition-colors duration-300 ease-in-out flex items-center justify-center space-x-2"
                    >
                      <FolderPlus className="w-5 h-5" />
                      <span>Add Files</span>
                    </Button>
                    <Button
                        onClick={handleBatchProcess}
                        disabled={batchProcessing || batchFiles.length === 0}
                        className="bg-teal-600 hover:bg-teal-700 text-white font-semibold py-2 px-4 rounded-lg transition-colors duration-300 ease-in-out flex items-center justify-center space-x-2"
                    >
                      {batchProcessing ? (
                          <>
                            <Loader2 className="w-5 h-5 animate-spin" />
                            <span>Processing...</span>
                          </>
                      ) : (
                          <>
                            <Play className="w-5 h-5" />
                            <span>Process Files</span>
                          </>
                      )}
                    </Button>
                  </div>
                  <div className="space-y-2">
                    {batchFiles.map((file, index) => (
                      <div key={index} className="flex items-center justify-between bg-muted p-2 rounded-md">
                        <span className="w-1/4 truncate">{file.name}</span>
                        <Select
                          value={file.pipeShape}
                          onValueChange={(value) => handleUpdateBatchFile(index, 'pipeShape', value)}
                        >
                          <SelectTrigger className="w-1/4">
                            <SelectValue placeholder="Pipe Shape" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="Circular">Circular</SelectItem>
                            <SelectItem value="Rectangular">Rectangular</SelectItem>
                            <SelectItem value="Egg Type 1">Egg Type 1</SelectItem>
                            <SelectItem value="Egg Type 2">Egg Type 2</SelectItem>
                            <SelectItem value="Egg Type 2A">Egg Type 2A</SelectItem>
                            <SelectItem value="Two Circle and Rectangle">Two Circle and Rectangle</SelectItem>
                          </SelectContent>
                        </Select>
                        <Input
                          className="w-1/4"
                          placeholder="Pipe Size"
                          value={file.pipeSize}
                          onChange={(e) => handleUpdateBatchFile(index, 'pipeSize', e.target.value)}
                        />
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => handleRemoveBatchFile(index)}
                          disabled={batchProcessing}
                        >
                          <X className="h-4 w-4" />
                        </Button>
                      </div>
                    ))}
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>

        <Card className="mt-4">
          <CardHeader>
            <CardTitle>Logs</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-40 bg-muted rounded-md p-2 overflow-auto">
              {logs.map((log, index) => (
                <div key={index} className={`${getLogColor(log.level)}`}>
                  {log.message}
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    </>
  );
};
