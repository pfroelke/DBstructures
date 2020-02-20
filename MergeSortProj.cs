using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;

namespace SBD_P1
{
    static class Globals
    {
        public static int DiskWriteCounter = 0;
        public static int DiskReadCounter = 0;
        public static int PhaseCounter = 0;
        public static int PageSize = 256;
        public static char Delimiter = ',';
    }

    class DigitSet : IComparable<DigitSet>
    {
        private string _digitsString;

        public DigitSet(string digitsString)
        {
            _digitsString = digitsString.Replace(Globals.Delimiter.ToString(), "");
        }

        public void SortSelf()
        {
            _digitsString = string.Concat(_digitsString.OrderByDescending(x => x));
        }

        public override string ToString()
        {
            return string.Join(Globals.Delimiter, _digitsString.ToCharArray());
        }

        public int CompareTo([AllowNull] DigitSet other)
        {
            if (other == null) return 1;
            if (_digitsString.Length < other._digitsString.Length) return -1;
            if (_digitsString.Length > other._digitsString.Length) return 1;

            for (int i = 0; i < _digitsString.Length; i++)
            {
                if (_digitsString[i] < other._digitsString[i]) return -1;
                if (_digitsString[i] > other._digitsString[i]) return 1;
            }

            return 0;
        }
    }

    struct SourceTape
    {
        private readonly DigitSet[] _page;
        private int _pageIndex;
        private int _recordIndex;
        private readonly string _fileName;
        private long _recordsInFile;

        public SourceTape(string fileName)
        {
            _page = new DigitSet[Globals.PageSize];
            _pageIndex = -1;
            _recordIndex = 0;
            _recordsInFile = 0;
            _fileName = fileName;
            LoadNextPage();
        }

        public void LoadNextPage()
        {
            _pageIndex++;
            _recordIndex = 0;
            using StreamReader reader = new StreamReader(_fileName);
            for (int i = 0; i < _pageIndex * Globals.PageSize; i++)
                reader.ReadLine();
            for (int i = 0; i < Globals.PageSize && reader.Peek() >= 0; i++)
            {
                string line = reader.ReadLine();
                _page[i] = new DigitSet(line);
                _recordsInFile++;
            }

            Globals.DiskReadCounter++;
        }

        public DigitSet LoadNextRecord()
        {
            if (_recordIndex == Globals.PageSize) LoadNextPage();
            if (_pageIndex * Globals.PageSize + _recordIndex + 1 > _recordsInFile) return null;
            _recordIndex++;
            return _page[_recordIndex - 1];
        }
    }

    struct DestinationTape : IDisposable
    {
        private readonly DigitSet[] _page;
        private int _recordIndex;
        private readonly string _fileName;
        public DestinationTape(string fileName)
        {
            _page = new DigitSet[Globals.PageSize];
            _recordIndex = 0;
            _fileName = fileName;
            File.Delete(fileName);
        }

        void SaveNextPage()
        {
            using StreamWriter writer = new StreamWriter(_fileName, true);
            for (int i = 0; i < _recordIndex; i++)
            {
                writer.WriteLine(_page[i]);
            }

            _recordIndex = 0;
            Globals.DiskWriteCounter++;
        }

        public void SaveNextRecord(DigitSet digitSet)
        {
            _page[_recordIndex] = digitSet;
            _recordIndex++;
            if (_recordIndex >= Globals.PageSize)
            {
                SaveNextPage();
            }
        }

        public void Dispose()
        {
            SaveNextPage();
        }
    }

    class Program
    {
        static void MockFile()
        {
            Console.WriteLine("Enter file name:");
            string fileName = Console.ReadLine();
            using var mockTape = new DestinationTape(fileName);
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

                    digitsSet = digitsSet.OrderByDescending(x => x).ToArray();
                    mockTape.SaveNextRecord(new DigitSet(string.Join(Globals.Delimiter, digitsSet.ToArray())));
                }
            }
        }

        static void ReadRecords()
        {
            Console.WriteLine("Enter file name:");
            string fileName = Console.ReadLine();
            using var mockTape = new DestinationTape(fileName);
            Console.WriteLine("Enter the amount of records:");
            if (int.TryParse(Console.ReadLine(), out int counter))
            {
                for (int i = 0; i < counter; i++)
                {
                    DigitSet toSave = new DigitSet(Console.ReadLine());
                    toSave.SortSelf();
                    mockTape.SaveNextRecord(toSave);
                }
            }
        }

        static TimeSpan SortFile()
        {
            bool sorted = false;

            Console.WriteLine("Enter file name: ");
            var filename = Console.ReadLine();
            File.Copy(filename, "s1.txt");
            Console.WriteLine("Display file after every phase? - Y/n");
            var displayOption = Console.ReadLine()?.ToLower() == "y" ? true : false;
            Console.Clear();
            Console.WriteLine("File before sorting:");
            DisplayFile(filename);

            Stopwatch sw = new Stopwatch();
            sw.Start();

            while (!sorted)
            {
                Globals.PhaseCounter++;
                Distribute();
                sorted = !Merge();

                if (displayOption) {
                    Console.WriteLine("Phase #" + Globals.PhaseCounter);
                    DisplayFile("s1.txt");
                    //Console.WriteLine("Press any key to continue to next phase.");
                    //Console.ReadKey();  
                }
            }
            sw.Stop();

            Console.WriteLine("File after sorting:");
            DisplayFile("d1.txt");

            File.Delete("sorted_" + filename);
            File.Copy("d1.txt", "sorted_" + filename);
            File.Delete("s1.txt");
            File.Delete("d1.txt");
            File.Delete("d2.txt");
            return sw.Elapsed;
        }

        static void Distribute()
        {
            SourceTape s1 = new SourceTape("s1.txt");
            using DestinationTape d1 = new DestinationTape("d1.txt");
            using DestinationTape d2 = new DestinationTape("d2.txt");
            int counter = 0;
            var record = s1.LoadNextRecord();
            DigitSet lastRecord = null;
            while (record != null)
            {
                if (record.CompareTo(lastRecord) == -1)
                {
                    counter++;
                    counter %= 2;
                }
                if (counter == 0) d1.SaveNextRecord(record);
                else d2.SaveNextRecord(record);

                lastRecord = record;
                record = s1.LoadNextRecord();
            }
        }
        static bool Merge()
        {
            SourceTape s1 = new SourceTape("d1.txt");
            SourceTape s2 = new SourceTape("d2.txt");
            using DestinationTape d1 = new DestinationTape("s1.txt");
            DigitSet r1, r2, l1 = null, l2 = null;
            r1 = s1.LoadNextRecord();
            r2 = s2.LoadNextRecord();
            if (r2 == null) return false;
            while (!(r1 == null && r2 == null))
            {
                if (r1 != null && r2 != null)
                {
                    if (r1.CompareTo(l1) == -1)
                    {
                        while (r2 != null && r2.CompareTo(l2) == 1)
                        {
                            d1.SaveNextRecord(r2);
                            l2 = r2;
                            r2 = s2.LoadNextRecord();
                        }

                        l1 = null;
                        l2 = null;
                    }

                    else if (r2.CompareTo(l2) == -1)
                    {
                        while (r1 != null && r1.CompareTo(l1) == 1)
                        {
                            d1.SaveNextRecord(r1);
                            l1 = r1;
                            r1 = s1.LoadNextRecord();
                        }

                        l1 = null;
                        l2 = null;
                    }
                    else
                    {
                        if (r1.CompareTo(r2) == -1)
                        {
                            d1.SaveNextRecord(r1);
                            l1 = r1;
                            r1 = s1.LoadNextRecord();
                        }
                        else
                        {
                            d1.SaveNextRecord(r2);
                            l2 = r2;
                            r2 = s2.LoadNextRecord();
                        }
                    }
                }

                else if (r1 == null)
                {
                    while (r2 != null)
                    {
                        d1.SaveNextRecord(r2);
                        l2 = r2;
                        r2 = s2.LoadNextRecord();
                    }
                }

                else if (r2 == null)
                {
                    while (r1 != null)
                    {
                        d1.SaveNextRecord(r1);
                        l1 = r1;
                        r1 = s1.LoadNextRecord();
                    }
                }
            }

            return true;
        }

        //static bool Merge2()
        //{
        //    SourceTape s1 = new SourceTape("d1.txt");
        //    SourceTape s2 = new SourceTape("d2.txt");
        //    using DestinationTape d1 = new DestinationTape("s1.txt");
        //    DigitSet r1, r2, l1 = null, l2 = null;
        //    r1 = s1.LoadNextRecord();
        //    r2 = s2.LoadNextRecord();
        //    if (r2 == null) return false;
        //    while (!(r1 == null && r2 == null))
        //    {
        //        if (r1 != null && r1.CompareTo(r2) == 1)
        //        {
        //            //r1 > r2
        //            if (r2 != null && r2.CompareTo(l1) == 1)
        //            {
        //                d1.SaveNextRecord(r2);
        //                l2 = r2;
        //                r2 = s2.LoadNextRecord();
        //            }
        //            else
        //            {
        //                d1.SaveNextRecord(r1);
        //                l1 = r1;
        //                r1 = s1.LoadNextRecord();
        //            }
        //        }
        //        else
        //        {
        //            if (r1 != null && r1.CompareTo(l2) == 1)
        //            {
        //                d1.SaveNextRecord(r1);
        //                l1 = r1;
        //                r1 = s1.LoadNextRecord();
        //            }
        //            else
        //            {
        //                d1.SaveNextRecord(r2);
        //                l2 = r2;
        //                r2 = s2.LoadNextRecord();
        //            }
        //        }
        //    }

        //    return true;
        //}

        static void ChangePageSize()
        {
            Console.Clear();
            Console.WriteLine("Current page size: " + Globals.PageSize);
            Console.Write("Enter new page size: ");

            if (int.TryParse(Console.ReadLine(), out int NewPageSize))
            {
                if (NewPageSize > 0)
                {
                    Globals.PageSize = NewPageSize;
                    Console.WriteLine("Page size changed to: " + Globals.PageSize);
                }
                else
                {
                    Console.WriteLine("Page size can not be negative!");
                }
            }
            else
            {

                Console.WriteLine("Invalid input!");
            }
        }

        static string DisplayMainMenu()
        {
            Console.Clear();
            Console.WriteLine("1 - Sort records from file");
            Console.WriteLine("2 - Generate file with random records");
            Console.WriteLine("3 - Sort entered records");
            Console.WriteLine("4 - Change page size");
            Console.WriteLine("5 - Exit");
            return Console.ReadLine();
        }

        static bool OptionController(string option)
        {
            Console.Clear();
            switch (option)
            {
                case "1":
                    TimeSpan t = SortFile();
                    DisplayDetails(t);
                    ResetGlobals();
                    break;
                case "2":
                    MockFile();
                    ResetGlobals();
                    Console.WriteLine("Records generated!");
                    break;
                case "3":
                    ReadRecords();
                    Console.WriteLine("Records saved!");
                    break;
                case "4":
                    ChangePageSize();
                    break;
                case "5":
                    return false;
                default:
                    Console.WriteLine("Invalid option");
                    break;
            }
            Console.ReadLine();
            return true;
        }

        static void DisplayDetails(TimeSpan t)
        {
            Console.WriteLine("Records sorted!");
            Console.WriteLine("Phases: " + --Globals.PhaseCounter);
            Console.WriteLine("Write operations: " + Globals.DiskWriteCounter);
            Console.WriteLine("Read operations: " + Globals.DiskReadCounter);
            Console.WriteLine("Time elapsed: " + t);
        }
        static void ResetGlobals()
        {
            Globals.DiskReadCounter = 0;
            Globals.DiskWriteCounter = 0;
            Globals.PhaseCounter = 0;
        }

        static void DisplayFile(string filename)
        {
            Console.WriteLine(File.ReadAllText(filename));
        }

        static void Main()
        {
            bool running = true;
            while (running)
            {
                var option = DisplayMainMenu();
                running = OptionController(option);
            }
        }
    }
}
