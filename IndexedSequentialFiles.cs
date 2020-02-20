using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace SBD_P2_V2
{
    static class Globals
    {
        public static string DbName;
        public static int DiskWriteCounter;
        public static int DiskReadCounter;
        public static int PageSize = 10;
        public static char Delimiter = ',';
        public static string DataFile;
        public static string IndexFile;
        public static double Alpha = 0.5;
        public static int CurrentPrimary;
        public static int CurrentOverflow;
        public static int MaxPrimary = 10;
        public static int MaxOverflow = MaxPrimary / 5;
        public static int NextKey = 1;
        public static int DigitSetSize = 30;
        public static int RecordSize = DigitSetSize + 8;
    }

    struct Index
    {
        public Index(byte[] bytes)
        {
            Key = BitConverter.ToInt32(bytes, 0);
            PageNumber = BitConverter.ToInt32(bytes, 4);
        }

        public int Key { get; set; }

        public int PageNumber { get; set; }

        public override string ToString()
        {
            return Key + "\t" + PageNumber;
        }
        public byte[] ToBytes()
        {
            int[] elements = { Key, PageNumber };
            byte[] bytes = new byte[elements.Length * sizeof(int)];
            Buffer.BlockCopy(elements, 0, bytes, 0, bytes.Length);

            return bytes;
        }
    }

    struct DigitSet : IComparable<DigitSet>
    {
        private string _digitsString;

        public string DigitsString => _digitsString;

        public DigitSet(string digitsString)
        {
            _digitsString = digitsString;
            Key = Globals.NextKey++;
            Pointer = 0;
        }

        public DigitSet(byte[] bytes)
        {
            _digitsString = Encoding.UTF8.GetString(bytes.Take(Globals.DigitSetSize).ToArray()).TrimEnd((char)0);
            if (_digitsString.Length == 0)
            {
                _digitsString = null;
            }
            Key = BitConverter.ToInt32(bytes, Globals.DigitSetSize);
            Pointer = BitConverter.ToInt32(bytes, Globals.DigitSetSize + 4);
        }

        public void SortSelf()
        {
            _digitsString = string.Concat(_digitsString.OrderBy(x => x));
        }

        public override string ToString()
        {
            if (_digitsString == null)
            {
                return "Record doesn't exist!";
            }
            return Key + "\t" + string.Join(Globals.Delimiter, _digitsString.ToCharArray());
        }

        public byte[] ToBytes()
        {
            byte[] bytes = _digitsString != null ? Encoding.UTF8.GetBytes(_digitsString) : new byte[Globals.RecordSize];
            Array.Resize(ref bytes, Globals.RecordSize);
            byte[] keyBytes = BitConverter.GetBytes(Key);
            Array.Copy(keyBytes, 0, bytes, Globals.DigitSetSize, keyBytes.Length);
            byte[] pointerBytes = BitConverter.GetBytes(Pointer);
            Array.Copy(pointerBytes, 0, bytes, Globals.DigitSetSize + keyBytes.Length, pointerBytes.Length);

            return bytes;
        }


        public int CompareTo(DigitSet other)
        {
            if (_digitsString == null) return 1;
            if (other._digitsString == null) return -1;
            if (_digitsString.Length < other._digitsString.Length) return -1;
            if (_digitsString.Length > other._digitsString.Length) return 1;

            for (int i = 0; i < _digitsString.Length; i++)
            {
                if (_digitsString[i] < other._digitsString[i]) return -1;
                if (_digitsString[i] > other._digitsString[i]) return 1;
            }

            return 0;
        }

        public int Key { get; set; }

        public int Pointer { get; set; }
    }

    class Program
    {
        static void LoadDbFile()
        {
            Console.Clear();
            Console.Write("Enter DB Name: ");
            Globals.DbName = Console.ReadLine();
            Globals.IndexFile = Globals.DbName + "Index.papi";
            Globals.DataFile = Globals.DbName + "Data.papi";

            if (File.Exists(Globals.DbName + ".papi"))
            {
                using StreamReader reader = new StreamReader(Globals.DbName + ".papi");
                char.TryParse(reader.ReadLine(), out Globals.Delimiter);
                double.TryParse(reader.ReadLine(), out Globals.Alpha);
                int.TryParse(reader.ReadLine(), out Globals.PageSize);
                int.TryParse(reader.ReadLine(), out Globals.CurrentPrimary);
                int.TryParse(reader.ReadLine(), out Globals.CurrentOverflow);
                int.TryParse(reader.ReadLine(), out Globals.MaxPrimary);
                int.TryParse(reader.ReadLine(), out Globals.MaxOverflow);
                int.TryParse(reader.ReadLine(), out Globals.NextKey);
                reader.Close();
            }

            File.Open(Globals.DbName + ".papi", FileMode.OpenOrCreate).Close();
            File.Open(Globals.IndexFile, FileMode.OpenOrCreate).Close();
            File.Open(Globals.DataFile, FileMode.OpenOrCreate).Close();

        }
        static void SaveDbFile()
        {
            using StreamWriter writer = new StreamWriter(Globals.DbName + ".papi");
            writer.WriteLine(Globals.Delimiter);
            writer.WriteLine(Globals.Alpha);
            writer.WriteLine(Globals.PageSize);
            writer.WriteLine(Globals.CurrentPrimary);
            writer.WriteLine(Globals.CurrentOverflow);
            writer.WriteLine(Globals.MaxPrimary);
            writer.WriteLine(Globals.MaxOverflow);
            writer.WriteLine(Globals.NextKey);
            writer.Close();
        }
        static void ResetGlobals()
        {
            Globals.DiskReadCounter = 0;
            Globals.DiskWriteCounter = 0;
        }

        static void ResetRecordPointers(ref DigitSet[] recSet, int n)
        {
            for (int i = 0; i < n; i++)
            {
                recSet[i].Pointer = 0;
            }
        }

        static void DisplayDetails(TimeSpan t)
        {
            Console.WriteLine();
            Console.WriteLine("Write operations: " + Globals.DiskWriteCounter);
            Console.WriteLine("Read operations: " + Globals.DiskReadCounter);
            Console.WriteLine("Time elapsed: " + t);
            Console.WriteLine("Records in DB: " + (Globals.CurrentOverflow + Globals.CurrentPrimary));
            Console.WriteLine("Max records in DB: " + (Globals.MaxOverflow + Globals.MaxPrimary));
        }

        static TimeSpan DisplayIndexes()
        {
            using FileStream indexReader = File.Open(Globals.IndexFile, FileMode.Open);
            int primaryIndexPages = (int)Math.Ceiling(Globals.MaxPrimary / (double)(Globals.PageSize * Globals.PageSize));
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Console.WriteLine("Indexes:");
            for (int i = 0; i < primaryIndexPages; i++)
            {
                var indexBuffer = ReadIndexPage(Globals.PageSize, indexReader);
                Console.WriteLine("Page " + i + ":");

                for (int j = 0; j < Globals.PageSize; j++)
                {
                    if (indexBuffer[j].Key == 0)
                    {
                        break;
                    }

                    Console.WriteLine("\t" + indexBuffer[j]);
                }
            }
            indexReader.Close();
            sw.Stop();
            return sw.Elapsed;
        }

        static TimeSpan DisplayRecords()
        {
            using FileStream dataReader = File.Open(Globals.DataFile, FileMode.Open);
            int primaryPages = (int)Math.Ceiling((Globals.MaxPrimary) / (double)Globals.PageSize);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            Console.WriteLine("Records:");
            for (int i = 0; i < primaryPages; i++)
            {
                var recordBuffer = ReadRecordPage(Globals.PageSize, dataReader);
                Console.WriteLine("Page " + i + ":");

                for (int j = 0; j < Globals.PageSize; j++)
                {
                    if (recordBuffer[j].DigitsString == null)
                    {
                        break;
                    }

                    Console.WriteLine("\t" + recordBuffer[j]);
                    if (recordBuffer[j].Pointer != 0)
                    {
                        var currentPos = dataReader.Position;
                        dataReader.Seek(primaryPages * Globals.PageSize * Globals.RecordSize, SeekOrigin.Begin);
                        DigitSet tmp = recordBuffer[j];

                        DigitSet[] overflowBuffer;
                        do
                        {
                            overflowBuffer = ReadRecordPage(Globals.PageSize, dataReader);

                            for (int k = 0; k < Globals.PageSize; k++)
                            {
                                if (overflowBuffer[k].DigitsString == null)
                                {
                                    break;
                                }
                                if (tmp.Pointer != overflowBuffer[k].Key) continue;
                                Console.WriteLine("OF\t" + overflowBuffer[k]);
                                tmp = overflowBuffer[k];
                            }
                        } while (overflowBuffer[Globals.PageSize - 1].Key != 0);

                        dataReader.Seek(currentPos, SeekOrigin.Begin);
                    }
                }
            }
            dataReader.Close();
            sw.Stop();
            return sw.Elapsed;
        }
        static void MockFile()
        {
            Console.WriteLine("Enter the amount of records to be generated:");
            if (int.TryParse(Console.ReadLine(), out int counter))
            {
                var ranGen = new Random();
                for (int i = 0; i < counter; i++)
                {
                    int[] digitsSet = new int[ranGen.Next(1, 30)];

                    for (int j = 0; j < digitsSet.Length; j++)
                    {
                        digitsSet[j] = ranGen.Next(0, 10);
                    }

                    DigitSet toAdd = new DigitSet(string.Join("", digitsSet)) {Key = ranGen.Next()};
                    AddRecord(toAdd);
                }
            }
        }

        static void LoadTestFile(string path)
        {
            try
            {
                using var reader = new StreamReader(path);
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    string[] words = line.Split(" ", StringSplitOptions.RemoveEmptyEntries);

                    int key;
                    switch (words.Length)
                    {
                        case 1: // Reorganise
                            Reorganise();
                            break;
                        case 2: // Delete record
                            if (int.TryParse(words[1], out key))
                            {
                                DeleteRecord(key, out _);
                            }
                            break;
                        case 3:
                            if (int.TryParse(words[1], out key))
                            {
                                DigitSet toSave = new DigitSet(words[2])
                                {
                                    Key = key
                                };
                                if (words[0] == "A") // Add record
                                {
                                    AddRecord(toSave);
                                }
                                else // Edit record
                                {
                                    UpdateRecord(toSave, out _);
                                }
                            }
                            break;
                    }
                }
            }
            catch (IOException e)
            {
                Console.WriteLine("The file could not be read:");
                Console.WriteLine(e.Message);
            }
        }
        static string DisplayMainMenu()
        {
            Console.Clear();
            Console.WriteLine("1 - Add record");
            Console.WriteLine("2 - Edit record");
            Console.WriteLine("3 - View record");
            Console.WriteLine("4 - Delete record");
            Console.WriteLine("5 - Reorganise");
            Console.WriteLine("6 - Display record file");
            Console.WriteLine("7 - Display index file");
            Console.WriteLine("8 - Generate Records");
            Console.WriteLine("9 - Load test file");
            Console.WriteLine("10 - Exit");
            return Console.ReadLine();
        }

        static bool OptionController(string option)
        {
            Console.Clear();
            TimeSpan elapsed;

            switch (option)
            {
                case "1":
                    DigitSet toSave;

                    Console.WriteLine("Enter digits (max. 30; ie. '12345').");
                    var input = Console.ReadLine();
                    if (Regex.IsMatch(input ?? throw new InvalidOperationException(), @"^\d+$"))
                    {
                        toSave = new DigitSet(input);
                        toSave.SortSelf();
                    }
                    else
                    {
                        Console.WriteLine("Invalid string!");
                        break;
                    }

                    Console.WriteLine("Enter record key.");
                    input = Console.ReadLine();
                    Console.Clear();
                    if (int.TryParse(input, out int key) && key != 0)
                    {
                        toSave.Key = key;
                        DigitSet existingRecord = SearchRecord(key);
                        if (existingRecord.DigitsString != null)
                        {
                            Console.WriteLine("Record with that key already exists!");
                            Console.WriteLine(existingRecord);
                            break;
                        }
                        Console.WriteLine("Record added!");
                        Console.WriteLine(toSave);
                        elapsed = AddRecord(toSave);
                        DisplayDetails(elapsed);
                        break;
                    }
                    Console.WriteLine("Invalid key!");
                    break;
                case "2":
                    Console.WriteLine("Enter digits (max. 30; ie. '12345').");
                    input = Console.ReadLine();
                    if (Regex.IsMatch(input ?? throw new InvalidOperationException(), @"^\d+$"))
                    {
                        toSave = new DigitSet(input);
                        toSave.SortSelf();
                    }
                    else
                    {
                        Console.WriteLine("Invalid string!");
                        break;
                    }

                    Console.WriteLine("Enter record key.");
                    input = Console.ReadLine();
                    Console.Clear();
                    if (int.TryParse(input, out key))
                    {
                        toSave.Key = key;
                        elapsed = UpdateRecord(toSave, out bool updated);
                        if (updated)
                        {
                            Console.WriteLine("Record updated!");
                            Console.WriteLine(toSave);
                        }
                        else
                        {
                            Console.WriteLine("Record not found!");
                        }
                        DisplayDetails(elapsed);
                        break;
                    }
                    Console.WriteLine("Invalid key!");
                    break;
                case "3":
                    Console.WriteLine("Enter record key.");
                    input = Console.ReadLine();
                    Console.Clear();
                    if (int.TryParse(input, out key))
                    {
                        Stopwatch sw = new Stopwatch();
                        sw.Start();
                        DigitSet existingRecord = SearchRecord(key);
                        if (existingRecord.DigitsString != null)
                        {
                            Console.WriteLine("Record found!");
                            Console.WriteLine(existingRecord);
                        }
                        else
                        {
                            Console.WriteLine("Record not found!");
                        }
                        sw.Stop();

                        DisplayDetails(sw.Elapsed);
                        break;
                    }
                    Console.WriteLine("Invalid key!");
                    break;
                case "4":
                    Console.WriteLine("Enter record key.");
                    input = Console.ReadLine();
                    Console.Clear();
                    if (int.TryParse(input, out key))
                    {
                        elapsed = DeleteRecord(key, out bool deleted);
                        Console.WriteLine(deleted ? "Record deleted!" : "Record not found!");
                        DisplayDetails(elapsed);
                        break;
                    }
                    Console.WriteLine("Invalid key!");
                    break;
                case "5":
                    elapsed = Reorganise();
                    DisplayDetails(elapsed);
                    break;
                case "6":
                    elapsed = DisplayRecords();
                    DisplayDetails(elapsed);
                    break;
                case "7":
                    elapsed = DisplayIndexes();
                    DisplayDetails(elapsed);
                    break;
                case "8":
                    Stopwatch sw2 = new Stopwatch();
                    sw2.Start();
                    MockFile();
                    sw2.Stop();
                    DisplayDetails(sw2.Elapsed);
                    break;
                case "9":
                    Console.WriteLine("Enter file path.");
                    input = Console.ReadLine();
                    Console.Clear();
                    LoadTestFile(input);
                    break;
                case "10":
                    SaveDbFile();
                    return false;
                default:
                    Console.WriteLine("Invalid option");
                    break;
            }
            ResetGlobals();
            Console.ReadLine();
            return true;
        }

        static void SortRecords(ref DigitSet[] ds)
        {
            ds = ds.OrderBy(x => x.Key == 0).ThenBy(x => x.Key).ToArray();
        }

        static int SearchIndex(int key)
        {
            using FileStream indexReader = File.Open(Globals.IndexFile, FileMode.Open);
            int primaryPages = (int)Math.Ceiling(Globals.MaxPrimary / (double)Globals.PageSize);
            int pageKey = 0, pageNumber = 0;

            for (int i = 0; i < primaryPages; i++)
            {
                var indexBuffer = ReadIndexPage(Globals.PageSize, indexReader);

                for (int j = 0; j < Globals.PageSize; j++)
                {
                    if (indexBuffer[j].Key <= key && pageKey < indexBuffer[j].Key)
                    {
                        pageNumber = indexBuffer[j].PageNumber;
                        pageKey = indexBuffer[j].Key;
                    }
                }

                if (pageNumber == 0)
                {
                    break;
                }
            }

            return pageNumber;
        }

        static void CreateIndex()
        {
            int i, j;
            Index[] indexBuffer = new Index[Globals.PageSize];

            using FileStream indexWriter = File.Open(Globals.IndexFile, FileMode.Open);
            using FileStream dataReader = File.Open(Globals.DataFile, FileMode.Open);
            int primaryPages = (int)Math.Ceiling(Globals.MaxPrimary / (double)Globals.PageSize);

            for (i = 0, j = 0; i < primaryPages; i++, j++)
            {
                if (j == Globals.PageSize)
                {
                    SaveIndexPage(indexBuffer, indexWriter);
                    j = 0;
                }

                var recordBuffer = ReadRecordPage(Globals.PageSize, dataReader);
                indexBuffer[j].Key = recordBuffer[0].Key;
                indexBuffer[j].PageNumber = i;
            }
            SaveIndexPage(indexBuffer, indexWriter);
        }

        static void SaveRecord(DigitSet rec, Stream str)
        {
            byte[] bytes = rec.ToBytes();
            foreach (byte b in bytes)
            {
                str.WriteByte(b);
            }
        }

        static void SaveRecordPage(DigitSet[] recPage, Stream str)
        {
            foreach (var rec in recPage)
            {
                SaveRecord(rec, str);
            }
            Globals.DiskWriteCounter++;
        }

        static void SaveIndex(Index i, Stream str)
        {
            byte[] bytes = i.ToBytes();
            foreach (byte b in bytes)
            {
                str.WriteByte(b);
            }
        }

        static void SaveIndexPage(Index[] iPage, Stream str)
        {
            foreach (var rec in iPage)
            {
                SaveIndex(rec, str);
            }
            Globals.DiskWriteCounter++;
        }

        static DigitSet ReadRecord(Stream str)
        {
            byte[] bytes = ReadBytes(Globals.RecordSize, str);
            return new DigitSet(bytes);
        }

        static DigitSet[] ReadRecordPage(int n, Stream str)
        {
            DigitSet[] data = new DigitSet[n];

            for (int i = 0; i < n; i++)
            {
                data[i] = ReadRecord(str);
            }

            Globals.DiskReadCounter++;
            return data;
        }

        static Index ReadIndex(Stream str)
        {
            byte[] bytes = ReadBytes(8, str);
            return new Index(bytes);
        }

        static Index[] ReadIndexPage(int n, Stream str)
        {
            Index[] data = new Index[n];

            for (int i = 0; i < n; i++)
            {
                data[i] = ReadIndex(str);
            }

            Globals.DiskReadCounter++;
            return data;
        }

        static byte[] ReadBytes(int n, Stream str)
        {
            byte[] bytes = new byte[n];

            if (str.Length == 0 || str.Position == str.Length)
            {
                return bytes;
            }

            for (int i = 0; i < n; i++)
            {
                bytes[i] = (byte)str.ReadByte();
            }

            return bytes;
        }

        static TimeSpan AddRecord(DigitSet record)
        {
            if (Globals.CurrentOverflow == Globals.MaxOverflow)
            {
                Reorganise();
            }

            DigitSet[] recordBuffer;
            int pageNumber = SearchIndex(record.Key);
            using FileStream dataReader = File.Open(Globals.DataFile, FileMode.Open);
            using FileStream tmpWriter = File.Open("tmp.papi", FileMode.OpenOrCreate);
            bool createIndex = false;
            Stopwatch sw = new Stopwatch();
            sw.Start();

            // Copy first part of primary area
            for (int i = 0; i < pageNumber; i++)
            {
                recordBuffer = ReadRecordPage(Globals.PageSize, dataReader);
                SaveRecordPage(recordBuffer, tmpWriter);
            }

            // Load destination page
            recordBuffer = ReadRecordPage(Globals.PageSize, dataReader);

            // If page not full insert record
            if (recordBuffer[Globals.PageSize - 1].Key == 0)
            {
                // Traverse records in page
                for (int i = 0; i < Globals.PageSize; i++)
                {
                    if (recordBuffer[i].Key == record.Key)
                    {
                        Console.WriteLine("Record with that key already exists!");
                        dataReader.Close();
                        tmpWriter.Close();
                        File.Delete("tmp.papi");
                        sw.Stop();
                        return sw.Elapsed;
                    }
                    // Found empty slot
                    if (recordBuffer[i].Key == 0)
                    {
                        // If its the first record of the page, index it
                        if (record.Key <= recordBuffer[0].Key)
                        {
                            createIndex = true;
                        }

                        // Insert record and save page
                        recordBuffer[i] = record;
                        Globals.CurrentPrimary++;
                        SortRecords(ref recordBuffer);
                        SaveRecordPage(recordBuffer, tmpWriter);

                        // Save the rest of primary area
                        int pageMax = (int)Math.Ceiling((Globals.MaxOverflow + Globals.MaxPrimary) / (double)Globals.PageSize);
                        for (i = pageNumber + 1; i < pageMax; i++)
                        {
                            recordBuffer = ReadRecordPage(Globals.PageSize, dataReader);
                            SaveRecordPage(recordBuffer, tmpWriter);
                        }

                        // Cleanup
                        dataReader.Close();
                        tmpWriter.Close();
                        File.Delete(Globals.DataFile); // Delete the existing file if exists
                        File.Move("tmp.papi", Globals.DataFile);
                        break;
                    }
                }

                if (createIndex)
                {
                    CreateIndex();
                }
            }
            // Else insert to overflow
            else
            {
                int i;
                for (i = 0; i < Globals.PageSize; i++)
                {
                    if (recordBuffer[i].Key == record.Key)
                    {
                        Console.WriteLine("Record with that key already exists!");
                        dataReader.Close();
                        tmpWriter.Close();
                        File.Delete("tmp.papi");
                        sw.Stop();
                        return sw.Elapsed;
                    }

                    if (recordBuffer[i].Key > record.Key)
                    {
                        break;
                    }
                }

                if (i != 0) i--;
                var ptr = recordBuffer[i].Pointer;
                if (ptr == 0 || ptr > record.Key)
                {
                    recordBuffer[i].Pointer = record.Key;
                }

                SaveRecordPage(recordBuffer, tmpWriter);

                int pageMax = (int)Math.Ceiling(Globals.MaxPrimary / (double)Globals.PageSize);

                for (i = pageNumber + 1; i < pageMax; i++)
                {
                    recordBuffer = ReadRecordPage(Globals.PageSize, dataReader);
                    SaveRecordPage(recordBuffer, tmpWriter);
                }

                int primaryPages = (int)Math.Ceiling((Globals.MaxPrimary) / (double)Globals.PageSize);
                DigitSet[] overflowBuffer;


                var currentPos = dataReader.Position;
                dataReader.Seek(primaryPages * Globals.PageSize * Globals.RecordSize, SeekOrigin.Begin);

                do
                {
                    overflowBuffer = ReadRecordPage(Globals.PageSize, dataReader);

                    for (int k = 0; k < Globals.PageSize; k++)
                    {
                        if (overflowBuffer[k].Key == 0)
                        {
                            record.Pointer = ptr;
                            overflowBuffer[k] = record;
                            SortRecords(ref overflowBuffer);
                            break;
                        }

                        if (overflowBuffer[k].Key == ptr)
                        {
                            if (overflowBuffer[k].Key < record.Key)
                            {
                                if (overflowBuffer[k].Pointer == 0)
                                {
                                    overflowBuffer[k].Pointer = record.Key;
                                    ptr = 0;
                                }
                                else if (overflowBuffer[k].Pointer > record.Key)
                                {
                                    ptr = overflowBuffer[k].Pointer;
                                    overflowBuffer[k].Pointer = record.Key;
                                }
                                else
                                {
                                    ptr = overflowBuffer[k].Pointer;
                                }

                            }
                            else
                            {
                                ptr = overflowBuffer[k].Key;
                            }
                        }
                    }
                } while (overflowBuffer[Globals.PageSize - 1].Key != 0);

                dataReader.Seek(currentPos, SeekOrigin.Begin);

                Globals.CurrentOverflow++;
                SaveRecordPage(overflowBuffer, tmpWriter);
                dataReader.Close();
                tmpWriter.Close();
                File.Delete(Globals.DataFile); // Delete the existing file if exists
                File.Move("tmp.papi", Globals.DataFile);
            }
            sw.Stop();
            return sw.Elapsed;
        }

        static TimeSpan DeleteRecord(int key, out bool deleted)
        {
            deleted = false;
            Stopwatch sw = new Stopwatch();
            sw.Start();

            if (key == 0)
            {
                Console.WriteLine("Can't delete record with key '0'!");
                sw.Stop();
                return sw.Elapsed;
            }

            DigitSet[] recordBuffer;
            int pageNumber = SearchIndex(key);
            using FileStream dataReader = File.Open(Globals.DataFile, FileMode.Open);
            using FileStream tmpWriter = File.Open("tmp.papi", FileMode.OpenOrCreate);
            bool recreateIndex = false;

            // Copy first part of primary area
            for (int i = 0; i < pageNumber; i++)
            {
                recordBuffer = ReadRecordPage(Globals.PageSize, dataReader);
                SaveRecordPage(recordBuffer, tmpWriter);
            }

            // Load destination page
            recordBuffer = ReadRecordPage(Globals.PageSize, dataReader);

            for (int i = 0; i < Globals.PageSize; i++)
            {
                // Record found
                if (recordBuffer[i].Key == key)
                {
                    // Delete record
                    recordBuffer[i] = new DigitSet();
                    Globals.CurrentPrimary--;
                    deleted = true;
                    if (i == 0)
                    {
                        recreateIndex = true;
                    }
                }

                // Not in primary area or deleted
                if (recordBuffer[i].DigitsString == null)
                {
                    break;
                }
            }
            // Delete from Overflow
            if (!deleted)
            {
                int primaryPages = (int)Math.Ceiling((Globals.MaxPrimary) / (double)Globals.PageSize);
                dataReader.Seek(primaryPages * Globals.PageSize * Globals.RecordSize, SeekOrigin.Begin);

                do
                {
                    recordBuffer = ReadRecordPage(Globals.PageSize, dataReader);

                    for (int i = 0; i < Globals.PageSize; i++)
                    {
                        if (recordBuffer[i].Key == key)
                        {
                            // Delete record
                            recordBuffer[i] = new DigitSet();
                            Globals.CurrentOverflow--;
                            deleted = true;
                        }
                    }
                } while (recordBuffer[Globals.PageSize - 1].Key != 0);
            }

            // Save page with deleted record
            SortRecords(ref recordBuffer);
            SaveRecordPage(recordBuffer, tmpWriter);

            // Save rest of primary area
            int pageMax = (int)Math.Ceiling((Globals.MaxOverflow + Globals.MaxPrimary) / (double)Globals.PageSize);
            for (int i = pageNumber + 1; i < pageMax; i++)
            {
                recordBuffer = ReadRecordPage(Globals.PageSize, dataReader);
                SaveRecordPage(recordBuffer, tmpWriter);
            }

            dataReader.Close();
            tmpWriter.Close();
            File.Delete(Globals.DataFile); // Delete the existing file if exists
            File.Move("tmp.papi", Globals.DataFile);

            if (recreateIndex)
            {
                CreateIndex();
            }

            sw.Stop();
            return sw.Elapsed;
        }

        static TimeSpan UpdateRecord(DigitSet record, out bool updated)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            updated = false;

            DeleteRecord(record.Key, out bool deleted);
            if (deleted)
            {
                AddRecord(record);
                updated = true;
            }

            sw.Stop();
            return sw.Elapsed;
        }
        static DigitSet SearchRecord(int key)
        {
            int pageNumber = SearchIndex(key), i;
            using FileStream dataReader = File.Open(Globals.DataFile, FileMode.Open);

            dataReader.Seek(pageNumber * Globals.RecordSize, SeekOrigin.Begin);

            DigitSet[] recordBuffer = ReadRecordPage(Globals.PageSize, dataReader);

            for (i = 0; i < Globals.PageSize; i++)
            {
                if (recordBuffer[i].Key == key)
                {
                    return recordBuffer[i];
                }
            }

            int primaryPages = (int)Math.Ceiling((Globals.MaxPrimary) / (double)Globals.PageSize);
            dataReader.Seek(primaryPages * Globals.PageSize * Globals.RecordSize, SeekOrigin.Begin);

            do
            {
                recordBuffer = ReadRecordPage(Globals.PageSize, dataReader);

                for (i = 0; i < Globals.PageSize; i++)
                {
                    if (recordBuffer[i].Key == key)
                    {
                        return recordBuffer[i];
                    }
                }
            } while (recordBuffer[Globals.PageSize - 1].Key != 0);

            dataReader.Close();
            return new DigitSet();
        }

        static TimeSpan Reorganise()
        {
            using FileStream dataReader = File.Open(Globals.DataFile, FileMode.Open);
            using FileStream tmpWriter = File.Open("tmp.papi", FileMode.OpenOrCreate);
            DigitSet[] tmpBuffer = new DigitSet[Globals.PageSize];
            int primaryPages = (int)Math.Ceiling(Globals.MaxPrimary / (double)Globals.PageSize);
            int currentRecordIndex = 0;
            int currentPages = 0;
            Stopwatch sw = new Stopwatch();
            sw.Start();

            for (int i = 0; i < primaryPages; i++)
            {
                var recordBuffer = ReadRecordPage(Globals.PageSize, dataReader);

                for (int j = 0; j < Globals.PageSize; j++)
                {
                    if (currentRecordIndex == (int)Math.Floor(Globals.PageSize * Globals.Alpha))
                    {
                        ResetRecordPointers(ref tmpBuffer, currentRecordIndex);
                        SaveRecordPage(tmpBuffer, tmpWriter);
                        currentRecordIndex = 0;
                        currentPages++;
                        tmpBuffer = new DigitSet[Globals.PageSize];
                    }

                    if (recordBuffer[j].DigitsString != null)
                    {
                        tmpBuffer[currentRecordIndex] = recordBuffer[j];
                        currentRecordIndex++;
                    }

                    var pointer = recordBuffer[j].Pointer;

                    if (pointer != 0)
                    {
                        var currentPos = dataReader.Position;
                        dataReader.Seek(primaryPages * Globals.PageSize * Globals.RecordSize, SeekOrigin.Begin);

                        DigitSet[] overflowBuffer;

                        do
                        {
                            overflowBuffer = ReadRecordPage(Globals.PageSize, dataReader);

                            for (int k = 0; k < Globals.PageSize && pointer != 0; k++, currentRecordIndex++)
                            {
                                if (currentRecordIndex == (int)Math.Floor(Globals.PageSize * Globals.Alpha))
                                {
                                    ResetRecordPointers(ref tmpBuffer, currentRecordIndex);
                                    SaveRecordPage(tmpBuffer, tmpWriter);
                                    currentRecordIndex = 0;
                                    currentPages++;
                                    tmpBuffer = new DigitSet[Globals.PageSize];
                                }

                                tmpBuffer[currentRecordIndex] = overflowBuffer[k];
                                pointer = overflowBuffer[k].Pointer;
                            }
                        } while (overflowBuffer[Globals.PageSize - 1].Key != 0);
                        dataReader.Seek(currentPos, SeekOrigin.Begin);
                    }
                }
            }

            if (currentRecordIndex > 0)
            {
                ResetRecordPointers(ref tmpBuffer, currentRecordIndex);
                SaveRecordPage(tmpBuffer, tmpWriter);
                currentPages++;
            }

            dataReader.Close();
            tmpWriter.Close();
            File.Delete(Globals.DataFile); // Delete the existing file if exists
            File.Move("tmp.papi", Globals.DataFile);

            Globals.MaxPrimary = currentPages * Globals.PageSize;
            Globals.MaxOverflow = Globals.MaxPrimary / 5;
            Globals.CurrentPrimary += Globals.CurrentOverflow;
            Globals.CurrentOverflow = 0;

            CreateIndex();

            sw.Stop();
            return sw.Elapsed;
        }

        static void Main()
        {
            bool running = true;
            LoadDbFile();
            while (running)
            {
                var option = DisplayMainMenu();
                running = OptionController(option);
            }
        }
    }
}
