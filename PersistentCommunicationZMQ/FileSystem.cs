using System;
using System.IO;

namespace PersistentCommunicationZMQ
{
    public abstract class FileSystem
    {
        private static object locker = new object();
        public static void WriteToFile(string fileName, Guid uid)
        {
            lock (locker)
            {
                try
                {
                    if (File.Exists(fileName))
                    {
                        if (!ContainsFile(fileName, uid))
                        {
                            using (StreamWriter writer = File.AppendText(fileName))
                            {
                                writer.WriteLine(uid.ToString());
                            }
                        }
                    }
                }
                catch (Exception exp)
                {
                    Console.Write(exp.Message);
                }
            }
        }
        public static void UpdateFile(string fileName, string uid)
        {
            lock (locker)
            {
                try
                {
                    if (File.Exists(fileName))
                    {
                        string text = File.ReadAllText(fileName);
                        text = text.Replace(uid + "\r\n", "");
                        File.WriteAllText(fileName, text);
                    }
                }
                catch (Exception exp)
                {
                    Console.Write(exp.Message);
                }
            }
        }
        public static bool ContainsFile(string fileName, Guid uid)
        {
            lock (locker)
            {
                try
                {
                    if (File.Exists(fileName))
                    {
                        using (StreamReader reader = new StreamReader(fileName))
                        {
                            string contents = reader.ReadToEnd();
                            if (contents.Contains(uid.ToString() + "\r\n"))
                            {
                                return true;
                            }
                        }
                    }
                }
                catch (Exception exp)
                {
                    Console.Write(exp.Message);
                }
            }
            return false;
        }
        public static void CreateFile(Guid uid, string content, string topic = null)
        {
            string filePath = ".\\Request\\" + uid + ".txt";

            lock (locker)
            {
                try
                {
                    using (StreamWriter writer = new StreamWriter(filePath))
                    {
                        if (topic != null)
                            writer.WriteLine(topic);
                        writer.WriteLine(content);
                    }
                }
                catch (Exception exp)
                {
                    Console.Write(exp.Message);
                }
            }
        }
        public static string ReadFile(string uid)
        {
            string filePath = Path.GetFullPath(".\\Response\\" + uid + ".txt");       
            string text="";

            lock (locker)
            {
                try
                {
                    if (File.Exists(filePath))
                    {
                        using (StreamReader reader = new StreamReader(filePath))
                        {
                            text = reader.ReadLine();
                        }
                    }
                }
                catch (Exception exp)
                {
                    Console.Write(exp.Message);
                }
            }
            return text;
        }
        public static void MoveFile(string uid, string text)
        {
            string filePath = ".\\Request\\" + uid + ".txt";
            lock (locker)
            {
                try
                {
                    if (File.Exists(filePath))
                    {
                        string newFilePath = filePath.Replace("Request", "Response");
                        File.Move(filePath, newFilePath);
                        using (StreamWriter writer = new StreamWriter(newFilePath))
                        {
                            writer.WriteLine(text);
                        }
                    }
                }
                catch (Exception exp)
                {
                    Console.Write(exp.Message);
                }
            }
        }
        public static void DeleteFile(string uid)
        {
            string filePath = Path.GetFullPath(".\\Response\\" + uid + ".txt");
            lock (locker)
            {
                try
                {
                    File.Delete(filePath);
                }
                catch (Exception exp)
                {
                    Console.Write(exp.Message);
                }
            }
        }
    }
}
