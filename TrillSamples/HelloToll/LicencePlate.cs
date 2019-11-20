using System;
using System.Collections.Generic;
using System.Text;

namespace HelloToll
{
  public class LicensePlate
  {
    public static string[] INVALID_PLATE_LETTERS = { "FOR", "AXE", "JAM", "JAB", "ZIP", "ARE", "YOU", "JUG", "JAW", "JOY" };
    private static Random Rand = new Random((int)DateTime.Now.Ticks);
    public static string GenerateLetters(int amount)
    {
      string letters = string.Empty;
      int n = 'Z' - 'A' + 1;
      for (int i = 0; i < amount; i++)
      {
        char c = (char)('A' + Rand.Next(n));
        letters += c;
      }
      return letters;
    }

    public static string GenerateDigits(int amount)
    {
      string digits = string.Empty;
      int n = '9' - '0' + 1;
      for (int i = 0; i < amount; i++)
      {
        char c = (char)('0' + Rand.Next(n));
        digits += c;
      }
      return digits;
    }

    public static string GenerateLicensePlate()
    {
      string licensePlate;
      string letters;
      do
      {
        letters = GenerateLetters(3);
      } while (IllegalWord(letters));

      string digits = GenerateDigits(3);

      licensePlate = letters + "-" + digits;
      return licensePlate;
    }

    private static bool IllegalWord(string letters)
    {
      for (int i = 0; i < INVALID_PLATE_LETTERS.Length; i++)
      {
        if (letters.Equals(INVALID_PLATE_LETTERS[i]))
        {
          return true;
        }
      }
      return false;
    }
  }
}
