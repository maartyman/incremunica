import jest from "jest-mock";
import expect from "expect";

//change timeout to 20 seconds
jasmine.DEFAULT_TIMEOUT_INTERVAL = 20000;

// Add missing Jest functions
window.test = window.it;
window.test.each = (inputs) => (testName, test) =>
    inputs.forEach((args) => window.it(testName, () => test(...args)));
window.test.todo = function () {
    return undefined;
};
window.jest = jest;
window.expect = expect;
