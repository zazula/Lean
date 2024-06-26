/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using NUnit.Framework;
using NUnit.Framework.Constraints;
using Python.Runtime;
using QuantConnect.Exceptions;
using System;
using System.Collections.Generic;

namespace QuantConnect.Tests.Common.Exceptions
{
    [TestFixture]
    public class ClrBubbledExceptionInterpreterTests
    {
        private string _pythonModuleName = "Test_PythonExceptionInterpreter";
        private ClrBubbledException _dotnetException;
        private PythonException _pythonException;

        [OneTimeSetUp]
        public void Setup()
        {
            using (Py.GIL())
            {
                var module = Py.Import(_pythonModuleName);
                dynamic algorithm = module.GetAttr("Test_PythonExceptionInterpreter").Invoke();

                try
                {
                    // self.MarketOrder(null, 1)
                    algorithm.dotnet_error();
                }
                catch (ClrBubbledException e)
                {
                    _dotnetException = e;
                }

                try
                {
                    // x = 1 / 0
                    algorithm.zero_division_error();
                }
                catch (PythonException e)
                {
                    _pythonException = e;
                }

                Assert.IsNotNull(_dotnetException);
                Assert.IsNotNull(_pythonException);
            }
        }

        [Test]
        [TestCase(typeof(Exception), ExpectedResult = false)]
        [TestCase(typeof(KeyNotFoundException), ExpectedResult = false)]
        [TestCase(typeof(DivideByZeroException), ExpectedResult = false)]
        [TestCase(typeof(InvalidOperationException), ExpectedResult = false)]
        [TestCase(typeof(PythonException), ExpectedResult = false)]
        [TestCase(typeof(ClrBubbledException), ExpectedResult = true)]
        public bool CanInterpretReturnsTrueOnlyForClrBubbledExceptionType(Type exceptionType)
        {
            var exception = CreateExceptionFromType(exceptionType);
            return new ClrBubbledExceptionInterpreter().CanInterpret(exception);
        }

        [Test]
        [TestCase(typeof(Exception), true)]
        [TestCase(typeof(KeyNotFoundException), true)]
        [TestCase(typeof(DivideByZeroException), true)]
        [TestCase(typeof(InvalidOperationException), true)]
        [TestCase(typeof(PythonException), true)]
        [TestCase(typeof(ClrBubbledException), false)]
        public void InterpretThrowsForNonClrBubbledExceptionTypes(Type exceptionType, bool expectThrow)
        {
            var exception = CreateExceptionFromType(exceptionType);
            var interpreter = new ClrBubbledExceptionInterpreter();
            var constraint = expectThrow ? (IResolveConstraint)Throws.Exception : Throws.Nothing;
            Assert.That(() => interpreter.Interpret(exception, NullExceptionInterpreter.Instance), constraint);
        }

        [Test]
        public void VerifyMessageContainsStackTraceInformation()
        {
            var exception = CreateExceptionFromType(typeof(ClrBubbledException));
            var assembly = typeof(ClrBubbledExceptionInterpreter).Assembly;
            var interpreter = StackExceptionInterpreter.CreateFromAssemblies(new[] { assembly });
            exception = interpreter.Interpret(exception, NullExceptionInterpreter.Instance);

            Assert.True(exception.Message.Contains("Value cannot be null. (Parameter 'key')", StringComparison.InvariantCulture));
            Assert.True(exception.Message.Contains("at dotnet_error", StringComparison.InvariantCulture));
            Assert.True(exception.Message.Contains("self.market_order(None", StringComparison.InvariantCulture));
            Assert.True(exception.Message.Contains($"in {_pythonModuleName}.py: line ", StringComparison.InvariantCulture));
        }

        private Exception CreateExceptionFromType(Type type)
        {
            if (type == typeof(ClrBubbledException))
            {
                return _dotnetException;
            }

            return type == typeof(PythonException) ? _pythonException : (Exception)Activator.CreateInstance(type);
        }
    }
}
