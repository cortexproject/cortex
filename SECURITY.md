# Cortex Security and Disclosure Information

As with any complex system, it is certain that bugs will be found, some of them security-relevant. If you find a _security bug_ please report it privately to the maintainers listed in the MAINTAINERS of the relevant repository and CC cortex-team@googlegroups.com. We will fix the issue as soon as possible and coordinate a release date with you. You will be able to choose if you want public acknowledgement of your effort and if you want to be mentioned by name.

## Public Disclosure Timing

The public disclosure date is agreed between the Cortex Team and the bug submitter. We prefer to fully disclose the bug as soon as possible, but only after a mitigation or fix is available. We will ask for delay if the bug or the fix is not yet fully understood or the solution is not tested to our standards yet. While there is no fixed timeframe for fix & disclosure, we will try our best to be quick and do not expect to need the usual 90 days most companies ask or. For a vulnerability with a straightforward mitigation, we expect report date to disclosure date to be on the order of 7 days.

---------------------------

## Private Vendors List

Cortex is being used by several vendors to provide a hosted Prometheus experience
to their users. We also have list that is used to provide actionable information
to multiple vendors at once. This list is not intended for individuals to find out about
security issues.

### Embargo Policy

The information members receive on cortex-vendors-announce@googlegroups.com
must not be made public, shared, nor even hinted at anywhere
beyond the need-to-know within your specific team except with the list's
explicit approval. This holds true until the public disclosure date/time that was
agreed upon by the list. Members of the list and others may not use the information
 for anything other than getting the issue fixed for your respective vendor’s users.

Before any information from the list is shared with respective members of your
team required to fix said issue, they must agree to the same terms and only
find out information on a need-to-know basis.

In the unfortunate event you share the information beyond what is allowed by
this policy, you _must_ urgently inform the cortex-team@googlegroups.com
mailing list of exactly what information
leaked and to whom. A retrospective will take place after the leak so
we can assess how to not make the same mistake in the future.

If you continue to leak information and break the policy outlined here, you
will be removed from the list.

### Contributing Back

This is a team effort. As a member of the list you must carry some water. This
could be in the form of the following:

**Technical**

- Review and/or test the proposed patches and point out potential issues with
  them (such as incomplete fixes for the originally reported issues, additional
  issues you might notice, and newly introduced bugs), and inform the list of the
  work done even if no issues were encountered.

**Administrative**

- Help draft emails to the public disclosure mailing list.
- Help with release notes.

### Membership

Group membership is managed by the Cortex maintainers using Google Groups.

### Membership Criteria

To be eligible for the cortex-vendors-announce@googlegroups.com mailing list, your
company should:

0. Have an actively monitored security email alias for our project.
1. Have a public hosted version/distribution of Cortex.
2. Have a user base not limited to your own organization.
4. Not be a downstream or rebuild of another vendor.
5. Be a participant and active contributor in the community.
6. Accept the [Embargo Policy](#embargo-policy) that is outlined above.
7. Be willing to [contribute back](#contributing-back) as outlined above.

**Removal**: If your vendor stops meeting one or more of these criteria
after joining the list then you will be unsubscribed.

### Request to Join

Send an email to cortex-team@googlegroups.com proving that you’re eligible based on the
above criteria. The team will review the application internally and will reach out to in
 case of any clarifications and will get back to you.